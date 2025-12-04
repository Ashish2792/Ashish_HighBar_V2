class CreativeGeneratorV2:
    """
    CreativeGeneratorV2

    Role:
        Implements the upgraded T5: `creative_generation` step.
        Generates diverse, CHS-aware ad creative suggestions for campaigns
        that look weak on CTR and/or Creative Health Score.

    Inputs:
        - data_summary:
            - campaign_summary
            - creative_summary
            - text_terms (per-campaign term frequencies)
        - chs_summary:
            - per-campaign CHS aggregates and component scores
        - hypotheses:
            - evaluated hypotheses from MetricEvaluator + CreativeEvaluator
              (used to prioritise which campaigns need new creatives)
        - params/config (optional):
            - variants_per_style: how many variants per style (default 3)
            - low_ctr_threshold: CTR below which a campaign is considered weak
            - chs_threshold: CHS below which a campaign is considered weak
            - max_campaigns: max campaigns to generate for
            - seed: random seed for reproducibility
            - overlap_threshold: Jaccard threshold vs existing copy
            - max_suggestions_per_campaign: hard cap on variants

    Behaviour:
        - Selects target campaigns based on:
            * creative-related hypotheses,
            * low CTR,
            * low CHS.
        - For each target campaign:
            * pulls top text terms and existing creative messages,
            * cleans/filters terms to avoid stopwords and noise,
            * generates variants across multiple styles:
                  - benefit, urgency, social_proof,
                    problem_solution, feature_highlight, audience_hook, relaxed
            * applies CHS-aware tweaks based on weak components:
                  - text_quality, fatigue, behavior
            * filters out high-overlap suggestions vs existing creatives,
              and runs a relaxed pass if too few suggestions are left.
        - Attaches metadata to each suggestion:
            * variant_style, core_term, overlap_score, risk_level
            * chs_targets, targeted_weakness
            * reasoning_chain: short explanation of why this creative exists
        - Builds a simple test_plan per campaign that can be used to structure A/B/C tests.

    Outputs:
        - {
            "creatives": [
                {
                    "campaign_name": ...,
                    "chs_current": ...,
                    "weak_components": [...],
                    "suggestions": [
                        {
                            "id": ...,
                            "headline": ...,
                            "message": ...,
                            "cta": ...,
                            "variant_style": ...,
                            "core_term": ...,
                            "chs_targets": [...],
                            "overlap_score": float,
                            "risk_level": "low" | "medium" | "high",
                            "reasoning_chain": [...]
                        },
                        ...
                    ],
                    "test_plan": { "control": ..., "variant_1": ..., ... }
                },
                ...
            ],
            "generated_at": ISO timestamp,
            "config_used": {...}
          }

    Assumptions:
        - Downstream consumers (Aggregator / report) only require the JSON
          structure described above; free-form text can evolve without breaking.
        - It is acceptable for some suggestions to be slightly unusual as long
          as they are safe and structurally correct; diversity is preferred
          over perfectly polished copy.
        - CHS is treated as directional signal, not as a hard constraint:
          large negative CHS deltas increase creative_confidence, positive
          deltas decrease it.
    """


from typing import Dict, Any, List, Optional, Tuple
import datetime
import random
import re
import math
import traceback

from src.utils.logger import AgentLogger
from src.utils.errors import CreativeGeneratorError, wrap_exc

DEFAULT_SEED = 2025

# Small helper to tokenize text for overlap
def _tokenize(text: str) -> List[str]:
    if not text:
        return []
    t = text.lower()
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    toks = [tok for tok in t.split() if len(tok) > 2]
    return toks

def _jaccard(a: str, b: str) -> float:
    ta = set(_tokenize(a))
    tb = set(_tokenize(b))
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)

class CreativeGeneratorV2:
    def __init__(self, config: Optional[Dict[str, Any]] = None, run_id: Optional[str] = None):
        """
        config options:
            variants_per_style: int (default 3)
            low_ctr_threshold: float
            chs_threshold: float
            max_campaigns: int
            seed: int
            overlap_threshold: float (0-1)
            max_suggestions_per_campaign: int
            min_term_count: int
            styles: optional list to override
        run_id: used by AgentLogger for per-run log file naming
        """
        self.config = {
            "variants_per_style": 3,
            "low_ctr_threshold": 0.02,
            "chs_threshold": 60.0,
            "max_campaigns": 10,
            "seed": DEFAULT_SEED,
            "overlap_threshold": 0.7,  # drop suggestions above this overlap
            "max_suggestions_per_campaign": 18,
            "min_term_count": 1,
            "styles": ["benefit", "urgency", "social_proof", "problem_solution", "feature_highlight", "audience_hook"],
        }
        if config:
            self.config.update(config)
        self.run_id = run_id
        self.logger = AgentLogger("CreativeGeneratorV2", run_id=self.run_id)
        # control randomness
        random.seed(self.config.get("seed", DEFAULT_SEED))
        self.logger.debug("init", "CreativeGeneratorV2 initialized", {"config": self.config})

        # Template banks (medium creative tone)
        # Each template is (headline_template, body_template)
        self.template_bank = {
            "benefit": [
                ("{TermCap}: comfort that keeps up", "Engineered {term} for all-day support and a barely-there feel — live comfortably."),
                ("Everyday {term} — effortless comfort", "Soft fabric, seamless design — made to disappear under clothes and stay comfy all day."),
                ("Move freely with {term}", "Built to flex with you. Breathable, light, and supportive where it counts.")
            ],
            "urgency": [
                ("Limited drop: {TermCap} in stock", "Fresh styles just landed — popular sizes moving fast. Grab yours before they’re gone."),
                ("Last chance for {term} deals", "Sale ends soon — restock your essentials while the offer lasts."),
                ("{TermCap} sale: ends tonight", "A small window for great comfort. Snag your size now.")
            ],
            "social_proof": [
                ("Loved by thousands: {TermCap}", "Rave reviews for fit and feel — higher reorder rates than category average."),
                ("Top-rated {term} for comfort", "Customers say it's the most comfortable they've owned — see the reviews."),
                ("Recommended pick: {TermCap}", "Our best-rated essential. Tried and trusted by real customers.")
            ],
            "problem_solution": [
                ("Tired of {pain}? Try {TermCap}", "We fixed chafing and fit — think soft edges and breathable fabric for daily comfort."),
                ("No more {pain} — meet {TermCap}", "A design built to eliminate common discomforts so you can focus on your day."),
                ("Say goodbye to {pain}", "{TermCap} is engineered for comfort and stability — an easy swap for a better day.")
            ],
            "feature_highlight": [
                ("{TermCap} with moisture-wicking tech", "Lightweight knit with advanced breathability — keeps you cool and dry."),
                ("Precision fit {term} — no bunching", "Flat seams and form-focused cuts make it invisible and dependable."),
                ("Durable {term} that stays put", "High-quality stretch for a consistent fit wash after wash.")
            ],
            "audience_hook": [
                ("Designed for {audience}", "A tailored fit for people who move a lot — perfect for athletes and busy days."),
                ("{TermCap} for minimalists", "Simple, effective design that fits every outfit and every day."),
                ("For those who value {value}", "If comfort and durability matter to you, these will become essentials.")
            ]
        }

        # CTA library (expanded, medium tone)
        self.ctas_by_style = {
            "benefit": ["See why customers love it", "Discover comfort", "Try comfort today"],
            "urgency": ["Grab yours", "Shop the drop", "Limited - shop now"],
            "social_proof": ["Read reviews", "See why it's top-rated", "Join thousands"],
            "problem_solution": ["Try the fix", "Solve it today", "See the solution"],
            "feature_highlight": ["Learn more", "See features", "Explore details"],
            "audience_hook": ["Find your fit", "Designed for you", "Explore the collection"],
            "default": ["Shop now", "See the collection", "View details", "Check availability", "Shop the drop"]
        }

    # ---------------- Public API ----------------

    def run_creative_generation(
        self,
        data_summary: Dict[str, Any],
        chs_summary: Dict[str, Dict[str, Any]],
        hypotheses: List[Dict[str, Any]],
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Entrypoint called by orchestrator.
        Returns a dict: {"creatives": [...], "generated_at": ..., "config_used": {...}}
        """
        self.logger.info("start", "CreativeGeneratorV2 run start", {"n_hypotheses": len(hypotheses)})
        try:
            if params:
                # merge safe params
                for k in ["variants_per_style", "low_ctr_threshold", "chs_threshold", "max_campaigns", "seed", "overlap_threshold", "max_suggestions_per_campaign"]:
                    if k in params:
                        self.config[k] = params[k]
                # reseed if provided
                random.seed(self.config.get("seed", DEFAULT_SEED))
                self.logger.debug("config_update", "Updated config from params", {"updated": list(params.keys())})

            # Prepare inputs
            campaign_summary = data_summary.get("campaign_summary", [])
            creative_summary = data_summary.get("creative_summary", [])
            text_terms = data_summary.get("text_terms", {})

            # Select candidate campaigns
            targets = self._select_targets(campaign_summary, chs_summary, hypotheses)
            self.logger.info("targets", "Selected target campaigns", {"count": len(targets), "targets": targets})

            # group existing messages per campaign
            existing_by_campaign = {}
            for row in creative_summary:
                cn = row.get("campaign_name")
                if cn:
                    existing_by_campaign.setdefault(cn, []).append(str(row.get("creative_message", "")))

            creatives_out = []
            for campaign in targets:
                try:
                    chs_info = chs_summary.get(campaign, {})
                    raw_terms = [t["term"] for t in text_terms.get(campaign, [])] if isinstance(text_terms.get(campaign), list) else []
                    existing_msgs = existing_by_campaign.get(campaign, [])

                    weak_components = self._infer_weak_components(chs_info)
                    suggestions = self._generate_for_campaign(
                        campaign_name=campaign,
                        terms=raw_terms,
                        existing_messages=existing_msgs,
                        weak_components=weak_components
                    )

                    if not suggestions:
                        self.logger.warn("no_suggestions", "No suggestions produced for campaign", {"campaign": campaign})
                        continue

                    # limit to configured max suggestions
                    max_sugg = int(self.config.get("max_suggestions_per_campaign", 18))
                    suggestions = suggestions[:max_sugg]

                    # Test plan: create an A/B/C... plan based on number of suggestions
                    test_plan = self._build_test_plan(len(suggestions))

                    creatives_out.append({
                        "campaign_name": campaign,
                        "chs_current": chs_info.get("chs_recent"),
                        "weak_components": weak_components,
                        "suggestions": suggestions,
                        "test_plan": test_plan
                    })

                    self.logger.info("campaign_generated", "Generated creatives for campaign", {"campaign": campaign, "n_suggestions": len(suggestions)})
                except Exception as e:
                    # Log and continue with other campaigns
                    self.logger.error("campaign_exception", "Failed generating creatives for campaign", {"campaign": campaign, "error": str(e), "trace": traceback.format_exc()})
                    continue

            self.logger.info("success", "CreativeGeneratorV2 completed", {"n_campaigns": len(creatives_out)})
            return {"creatives": creatives_out, "generated_at": datetime.datetime.utcnow().isoformat() + "Z", "config_used": self.config}
        except CreativeGeneratorError:
            self.logger.error("creative_error", "Known creative generator error", {"trace": traceback.format_exc()})
            raise
        except Exception as e:
            self.logger.error("exception", "Unhandled exception in CreativeGeneratorV2", {"trace": traceback.format_exc()})
            raise wrap_exc("CreativeGeneratorV2 failed", e, CreativeGeneratorError)

    # ---------------- Target selection ----------------

    def _select_targets(self, campaign_summary: List[Dict[str, Any]], chs_summary: Dict[str, Dict[str, Any]], hypotheses: List[Dict[str, Any]]) -> List[str]:
        """
        Prioritize campaigns flagged by creative hypotheses, low CTR, or low CHS.
        Rank by combined severity (chs deficit + ctr deficit + spend).
        """
        low_ctr = float(self.config.get("low_ctr_threshold", 0.02))
        chs_thresh = float(self.config.get("chs_threshold", 60.0))
        max_campaigns = int(self.config.get("max_campaigns", 10))

        target_set = set()

        # from creative-related hypotheses
        for h in hypotheses:
            if h.get("driver_type") == "creative" and h.get("campaign_name"):
                conf = h.get("creative_confidence", h.get("initial_confidence", 0.4))
                if conf >= 0.35:
                    target_set.add(h["campaign_name"])

        # low CTR & low CHS
        for cs in campaign_summary:
            cname = cs.get("campaign_name")
            if not cname:
                continue
            ctr = cs.get("ctr", 0.0)
            if ctr is not None and ctr < low_ctr:
                target_set.add(cname)

        for cname, chs in chs_summary.items():
            if not cname:
                continue
            chs_recent = chs.get("chs_recent")
            if chs_recent is not None and chs_recent < chs_thresh:
                target_set.add(cname)

        # Score and order
        scores = []
        chs_map = {k: v.get("chs_recent", None) for k, v in chs_summary.items()}
        ctr_map = {cs.get("campaign_name"): cs.get("ctr", 0.0) for cs in campaign_summary}
        spend_map = {cs.get("campaign_name"): cs.get("spend", 0.0) for cs in campaign_summary}

        for c in target_set:
            chs_val = chs_map.get(c, None)
            ctr_val = ctr_map.get(c, 0.0)
            spend_val = spend_map.get(c, 0.0)
            chs_pen = (100 - chs_val) if (chs_val is not None) else 0
            ctr_pen = max(0, (0.05 - ctr_val)) * 200  # accentuate very low CTR
            score = chs_pen + ctr_pen + spend_val / 10.0
            scores.append((c, score))

        scores.sort(key=lambda x: x[1], reverse=True)
        ordered = [c for c, _ in scores][:max_campaigns]
        return ordered

    # ---------------- Weak component inference ----------------

    def _infer_weak_components(self, chs_info: Optional[Dict[str, Any]]) -> List[str]:
        if not chs_info:
            return ["text_quality"]
        comps = []
        if chs_info.get("text_quality", 0.5) < 0.6:
            comps.append("text_quality")
        if chs_info.get("fatigue_score", 0.5) < 0.6:
            comps.append("fatigue")
        if chs_info.get("behavior_recent", 0.5) < 0.6:
            comps.append("behavior")
        return comps or ["text_quality"]

    # ---------------- Generation for a campaign ----------------

    def _generate_for_campaign(self, campaign_name: str, terms: List[str], existing_messages: List[str], weak_components: List[str]) -> List[Dict[str, Any]]:
        """
        Generate a diverse set of creatives for a campaign.
        Strategy:
            - For each style produce variants_per_style variants (typically 3)
            - Apply CHS-aware tweaks per weak component
            - Filter duplicates by overlap threshold
            - Attach reasoning_chain explaining why the creative targets weak components
        """
        variants_per_style = int(self.config.get("variants_per_style", 3))
        overlap_thresh = float(self.config.get("overlap_threshold", 0.7))

        # TERM CLEANING (new):
        raw_terms = [t for t in terms if isinstance(t, str)]
        stopwords = {"our", "with", "for", "the", "and", "a", "an", "in", "on", "of", "to", "from", "by", "you", "your", "this", "that"}
        clean_terms: List[str] = []
        for t in raw_terms:
            t_clean = t.strip().lower()
            # drop tokens that are stopwords, shorter than 3, or contain non-alpha heavy noise
            if not t_clean or len(t_clean) < 3:
                continue
            if t_clean in stopwords:
                continue
            # require at least 50% alphabetic chars
            alpha_frac = sum(c.isalpha() for c in t_clean) / max(1, len(t_clean))
            if alpha_frac < 0.5:
                continue
            clean_terms.append(t_clean)

        if not clean_terms:
            clean_terms = ["comfort", "fit", "soft"]
        # limit to top 8 terms
        terms = clean_terms[:8]
        # END TERM CLEANING

        existing_blob = " ".join(existing_messages) if existing_messages else ""
        styles = self.config.get("styles", ["benefit", "urgency", "social_proof", "problem_solution", "feature_highlight", "audience_hook"])

        suggestions: List[Dict[str, Any]] = []
        generated_set = set()

        # Iterate styles but randomize order for variety
        style_order = styles.copy()
        random.shuffle(style_order)

        for style in style_order:
            templates = self.template_bank.get(style, [])
            if not templates:
                continue
            # produce variants
            for v_i in range(variants_per_style):
                try:
                    templ_head, templ_body = random.choice(templates)
                    term = random.choice(terms)
                    # pain term for problem_solution
                    pain = random.choice(["chafing", "riding up", "bunching", "noticeable lines", "uneven fit"])
                    headline = templ_head.format(term=term, TermCap=term.capitalize(), pain=pain)
                    body = templ_body.format(term=term, TermCap=term.capitalize(), pain=pain)

                    # CHS-aware tweak
                    body = self._chs_tweak(body, weak_components, style)

                    # choose CTA (style-aware)
                    cta_choices = self.ctas_by_style.get(style, self.ctas_by_style["default"])
                    cta = random.choice(cta_choices)

                    # compute overlap vs existing
                    overlap = _jaccard(headline + " " + body, existing_blob)

                    # if overlap too high, skip
                    if overlap >= overlap_thresh:
                        self.logger.debug("skip_overlap", "Skipping variant due to high overlap", {"campaign": campaign_name, "style": style, "overlap": overlap})
                        continue

                    # simple dedupe across generated suggestions
                    key = (style, headline.lower(), body.lower())
                    if key in generated_set:
                        continue
                    generated_set.add(key)

                    reasoning = self._build_reasoning_chain(campaign_name, style, term, weak_components)
                    risk = self._assess_risk(headline, body)

                    suggestion = {
                        "id": f"{campaign_name[:6].replace(' ','')}_{style[:3]}_{len(suggestions)+1}",
                        "headline": headline,
                        "message": body,
                        "cta": cta,
                        "variant_style": style,
                        "targeted_weakness": weak_components,
                        "core_term": term,
                        "overlap_score": float(overlap),
                        "reasoning_chain": reasoning,
                        "chs_targets": weak_components,
                        "risk_level": risk
                    }
                    suggestions.append(suggestion)
                except Exception as e:
                    self.logger.debug("gen_variant_error", "Failed to generate variant", {"campaign": campaign_name, "style": style, "error": str(e)})
                    continue

        # If still too few suggestions, try a short relaxed generation pass (without changing self.config)
        if len(suggestions) < 3:
            self.logger.debug("relax_overlap", "Relaxed generation invoked to produce more variants", {"campaign": campaign_name, "current": len(suggestions)})
            more = self._generate_for_campaign_relaxed(campaign_name, terms, existing_messages, weak_components, needed=(3 - len(suggestions)))
            # merge unique
            for s in more:
                if all(s["headline"] != ex["headline"] for ex in suggestions):
                    suggestions.append(s)

        # final shuffle for randomness/sampling
        random.shuffle(suggestions)
        return suggestions

    def _generate_for_campaign_relaxed(self, campaign_name, terms, existing_messages, weak_components, needed=3):
        """
        Short relaxed generation pass that ignores overlap threshold to fill a few slots.
        Uses local relaxed overlap threshold to avoid mutating self.config.
        """
        results = []
        existing_blob = " ".join(existing_messages) if existing_messages else ""
        styles = self.config.get("styles", [])
        templates_pool = []
        for style in styles:
            templates_pool.extend(self.template_bank.get(style, []))
        local_overlap_thresh = min(0.98, float(self.config.get("overlap_threshold", 0.7)) + 0.25)
        attempts = 0
        while len(results) < needed and attempts < needed * 10:
            attempts += 1
            try:
                templ = random.choice(templates_pool)
                term = random.choice(terms)
                head = templ[0].format(term=term, TermCap=term.capitalize(), pain=random.choice(["chafing","bunching","fit"]))
                body = templ[1].format(term=term, TermCap=term.capitalize(), pain=random.choice(["chafing","bunching","fit"]))
                # tweak for CHS
                body = self._chs_tweak(body, weak_components, "relaxed")
                overlap = _jaccard(head + " " + body, existing_blob)
                if overlap >= local_overlap_thresh:
                    continue
                reasoning = self._build_reasoning_chain(campaign_name, "relaxed", term, weak_components)
                res = {
                    "id": f"{campaign_name[:6]}_rel_{random.randint(1000,9999)}",
                    "headline": head,
                    "message": body,
                    "cta": random.choice(self.ctas_by_style.get("default")),
                    "variant_style": "relaxed",
                    "targeted_weakness": weak_components,
                    "core_term": term,
                    "overlap_score": float(overlap),
                    "reasoning_chain": reasoning,
                    "chs_targets": weak_components,
                    "risk_level": "low"
                }
                results.append(res)
            except Exception:
                continue
        return results

    # ---------------- Helpers ----------------

    def _chs_tweak(self, body: str, weak_components: List[str], style: str) -> str:
        """
        Modify body text slightly based on CHS weaknesses and the style.
        Aim: subtle but targeted adjustments so suggested creatives address weaknesses.
        """
        tweak = ""
        if "fatigue" in weak_components:
            # emphasize freshness / newness
            tweak += " Try a fresh colour or pattern to reduce ad fatigue. "
        if "text_quality" in weak_components:
            # add stronger benefit or proof line
            tweak += " Designed for comfort and built to last — feel it yourself."
        if "behavior" in weak_components and style in ("benefit", "feature_highlight"):
            tweak += " Tested for improved fit and conversion."
        if tweak:
            body = body + " " + tweak
        return body

    def _build_reasoning_chain(self, campaign: str, style: str, term: str, weak_components: List[str]) -> List[str]:
        """
        Short human-readable reasoning steps explaining why this creative was produced.
        """
        reasons = [
            f"Campaign='{campaign}' flagged for creative attention.",
            f"Style='{style}' chosen to target weaknesses: {', '.join(weak_components)}.",
            f"Core term='{term}' used from campaign's top keywords.",
            "Template selected to provide medium-tone messaging suitable for DTC undergarments."
        ]
        # If fatigue present, recommend freshness
        if "fatigue" in weak_components:
            reasons.append("Includes freshness cue to address creative fatigue.")
        if "text_quality" in weak_components:
            reasons.append("Adds benefit/proof language to improve click intent.")
        if "behavior" in weak_components:
            reasons.append("Includes performance-focused phrasing to drive conversions.")
        return reasons

    def _assess_risk(self, headline: str, body: str) -> str:
        """
        Heuristic rule to assess risk level of a creative:
        - low: safe, product-focused
        - medium: uses urgency or stronger claims
        - high: potential borderline claims (we avoid these)
        """
        headline_l = headline.lower()
        body_l = body.lower()
        if any(w in headline_l for w in ["sale", "limited", "last", "ends"]):
            return "medium"
        if any(w in body_l for w in ["guarantee", "cure", "never", "always"]):
            return "high"
        return "low"

    def _build_test_plan(self, n_variants: int) -> Dict[str, Any]:
        """
        Build an A/B/C... split plan across variants and normalize to 100%.
        Control is the first item (50% if >=3 variants).
        """
        plan = {}
        if n_variants <= 1:
            return {"control": 100}
        # If many variants, make control 50% (for >=3 variants), distribute remainder evenly
        if n_variants >= 3:
            plan["control"] = 50
            rem = 50
            per_variant = rem / (n_variants - 1)
            # produce integer percentages that sum to 100
            assigned = []
            for i in range(1, n_variants):
                assigned.append(int(math.floor(per_variant)))
            # adjust leftover
            leftover = rem - sum(assigned)
            i = 0
            while leftover > 0:
                assigned[i] += 1
                leftover -= 1
                i = (i + 1) % len(assigned)
            for idx, val in enumerate(assigned, start=1):
                plan[f"variant_{idx}"] = int(val)
        else:
            # two-way split roughly evenly
            base = int(100 // n_variants)
            for i in range(n_variants):
                plan[f"variant_{i+1}"] = base
            # adjust remainder
            leftover = 100 - sum(plan.values())
            i = 1
            while leftover > 0:
                plan[f"variant_{i}"] += 1
                leftover -= 1
                i = i + 1 if (i + 1) <= n_variants else 1
        return plan

    def _assure_term(self, term: str) -> str:
        return term if term else "comfort"
