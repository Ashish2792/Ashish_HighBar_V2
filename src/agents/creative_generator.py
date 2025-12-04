class CreativeGeneratorAgent:
    """
    CreativeGeneratorAgent

    Role:
        Implements T5: `creative_generation`.
        For campaigns with clear creative issues (low CTR or low CHS),
        generate grounded creative suggestions and a lightweight test plan.

    Inputs:
        - data_summary:
            - campaign_summary
            - creative_summary
            - text_terms
        - chs_summary: from CreativeEvaluatorAgent["chs_summary"].
        - hypotheses: evaluated hypotheses (may contain creative_confidence).
        - config/params:
            - variants_per_type
            - low_ctr_threshold
            - chs_threshold
            - max_campaigns

    Outputs:
        - {
            "creatives": [
                {
                    "campaign_name": ...,
                    "chs_current": ...,
                    "weak_components": {...},
                    "suggestions": [
                        {
                            "variant_id": ...,
                            "variant_type": "benefit" | "urgency" | "social_proof",
                            "headline": ...,
                            "primary_text": ...,
                            "cta": ...,
                            "tags": [...]
                        },
                        ...
                    ],
                    "test_plan": {...}
                },
                ...
            ],
            "generated_at": ISO timestamp,
            "config_used": {...}
          }

    Assumptions:
        - We prefer campaigns that have either low CTR or CHS below the
          configured threshold, and we cap the total number via `max_campaigns`.
        - Suggestions are grounded in `text_terms` and existing messages to
          avoid totally off-brand ideas.
        - Test plan is deliberately simple: 1 control + 2 variants with
          predefined traffic splits.
    """


from typing import Dict, Any, List, Optional, Set
import datetime
import re
from collections import defaultdict
import random
import traceback

# logging & errors
from src.utils.logger import AgentLogger
from src.utils.errors import CreativeGeneratorError, wrap_exc

class CreativeGeneratorAgent:
    def __init__(self, config: Optional[Dict[str, Any]] = None, run_id: Optional[str] = None):
        # keep original defaults but allow overrides
        self.config = {
            "variants_per_type": 3,
            "low_ctr_threshold": 0.02,
            "chs_threshold": 60.0,  # CHS below this considered weak
            "max_campaigns": 10,    # limit generation to top N campaigns
            "seed": 2025,
            "overlap_threshold": 0.75,  # drop suggestions that overlap too much with existing creatives
        }
        if config:
            self.config.update(config)
        self.run_id = run_id
        self.logger = AgentLogger("CreativeGenerator", run_id=self.run_id)
        random.seed(self.config.get("seed", 2025))

    # ---------- Public API ----------

    def run_creative_generation(
        self,
        data_summary: Dict[str, Any],
        chs_summary: Dict[str, Dict[str, Any]],
        hypotheses: List[Dict[str, Any]],
        params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Main entry point for T5: 'creative_generation'.
        """
        self.logger.info("start", "creative generation start", {"n_hypotheses": len(hypotheses)})
        try:
            if params:
                for k in ["variants_per_type", "low_ctr_threshold", "chs_threshold", "max_campaigns", "seed", "overlap_threshold"]:
                    if k in params:
                        self.config[k] = params[k]
                # reseed if seed provided
                random.seed(self.config.get("seed", 2025))
                self.logger.debug("config_update", "Updated creative generator config", {"updated_keys": list(params.keys())})

            campaign_summary = data_summary.get("campaign_summary", [])
            creative_summary = data_summary.get("creative_summary", [])
            text_terms = data_summary.get("text_terms", {})

            # 1) Identify target campaigns
            target_campaigns = self._select_target_campaigns(
                campaign_summary,
                chs_summary,
                hypotheses
            )
            self.logger.info("targets_selected", "Selected target campaigns for creative gen", {"count": len(target_campaigns), "targets": target_campaigns})

            # Group existing creative messages per campaign
            creatives_by_campaign: Dict[str, List[str]] = defaultdict(list)
            for row in creative_summary:
                creatives_by_campaign[row["campaign_name"]].append(str(row["creative_message"]))

            creatives_out: List[Dict[str, Any]] = []

            for cname in target_campaigns:
                try:
                    chs_info = chs_summary.get(cname, {})
                    terms = text_terms.get(cname, [])
                    existing_msgs = creatives_by_campaign.get(cname, [])

                    weak_components = self._infer_weak_components(chs_info)
                    suggestions = self._generate_for_campaign(
                        campaign_name=cname,
                        terms=terms,
                        existing_messages=existing_msgs,
                        weak_components=weak_components,
                        variants_per_type=self.config["variants_per_type"]
                    )

                    # filter suggestions that are too similar to existing creatives
                    overlap_thresh = float(self.config.get("overlap_threshold", 0.75))
                    filtered = [s for s in suggestions if s.get("overlap_score", 0.0) <= overlap_thresh]
                    dropped = len(suggestions) - len(filtered)
                    if dropped > 0:
                        self.logger.info("dedupe", "Dropped near-duplicate suggestions", {"campaign": cname, "dropped": dropped})

                    if not filtered:
                        self.logger.warn("no_suggestions", "No diverse suggestions generated for campaign", {"campaign": cname})
                        continue

                    # Simple A/B test plan: control + first two variants (if available)
                    test_plan = {
                        "control": 50,
                        "variant_1": 25,
                        "variant_2": 25
                    }

                    creatives_out.append({
                        "campaign_name": cname,
                        "chs_current": chs_info.get("chs_recent") if chs_info else None,
                        "weak_components": weak_components,
                        "suggestions": filtered,
                        "test_plan": test_plan
                    })
                    self.logger.info("campaign_done", "Generated creatives for campaign", {"campaign": cname, "n_suggestions": len(filtered)})
                except Exception as e:
                    # per-campaign failure should not stop whole run
                    self.logger.error("campaign_exception", f"Failed to generate creatives for {cname}", {"campaign": cname, "error": str(e), "trace": traceback.format_exc()})
                    # continue to next campaign

            result = {
                "creatives": creatives_out,
                "generated_at": datetime.datetime.utcnow().isoformat() + "Z",
                "config_used": self.config
            }
            self.logger.info("success", "creative generation completed", {"n_campaigns": len(creatives_out)})
            return result
        except CreativeGeneratorError:
            self.logger.error("creative_error", "Known creative error raised", {"trace": traceback.format_exc()})
            raise
        except Exception as e:
            self.logger.error("exception", "Unhandled exception in CreativeGenerator", {"trace": traceback.format_exc()})
            raise wrap_exc("CreativeGenerator failed during run_creative_generation", e, CreativeGeneratorError)

    # ---------- Target campaign selection ----------

    def _select_target_campaigns(
        self,
        campaign_summary: List[Dict[str, Any]],
        chs_summary: Dict[str, Dict[str, Any]],
        hypotheses: List[Dict[str, Any]],
    ) -> List[str]:
        """
        Combine:
        - creative-related hypotheses with decent creative_confidence
        - campaigns with low CTR
        - campaigns with low CHS
        """
        low_ctr_threshold = self.config["low_ctr_threshold"]
        chs_threshold = self.config["chs_threshold"]

        target: Set[str] = set()

        # 1) From hypotheses: creative-related & confident
        for h in hypotheses:
            if h.get("driver_type") == "creative" and h.get("campaign_name"):
                creative_conf = h.get("creative_confidence", h.get("initial_confidence", 0.0))
                if creative_conf >= 0.4:
                    target.add(h["campaign_name"])

        # 2) From low CTR campaigns
        for cs in campaign_summary:
            cname = cs["campaign_name"]
            ctr = cs.get("ctr")
            if ctr is not None and ctr < low_ctr_threshold:
                target.add(cname)

        # 3) From low CHS campaigns
        for cname, chs_info in chs_summary.items():
            chs_recent = chs_info.get("chs_recent")
            if chs_recent is not None and chs_recent < chs_threshold:
                target.add(cname)

        # Respect max_campaigns setting but keep stable order based on severity
        # Sort roughly by: lower CHS, lower CTR, higher spend.
        score_records = []
        chs_map = {k: v.get("chs_recent") for k, v in chs_summary.items()}
        ctr_map = {cs["campaign_name"]: cs.get("ctr") for cs in campaign_summary}
        spend_map = {cs["campaign_name"]: cs.get("spend") for cs in campaign_summary}

        for cname in target:
            chs_val = chs_map.get(cname, None)
            ctr_val = ctr_map.get(cname, None)
            spend_val = spend_map.get(cname, 0.0)
            # Lower CHS and lower CTR are worse ⇒ higher priority.
            chs_penalty = (100 - chs_val) if chs_val is not None else 0
            ctr_penalty = (1 - ctr_val) * 100 if ctr_val is not None else 0
            risk_score = chs_penalty + ctr_penalty + (spend_val or 0) / 10.0
            score_records.append((cname, risk_score))

        score_records.sort(key=lambda x: x[1], reverse=True)
        ordered = [c for c, _ in score_records][: self.config["max_campaigns"]]
        return ordered

    # ---------- Weak components from CHS ----------

    def _infer_weak_components(self, chs_info: Optional[Dict[str, Any]]) -> List[str]:
        if not chs_info:
            return ["text_quality"]  # default to improving copy

        comps = []
        text_q = chs_info.get("text_quality", 0.5)
        fatigue = chs_info.get("fatigue_score", 0.5)
        behavior_recent = chs_info.get("behavior_recent", 0.5)

        # Lower score ⇒ weaker area
        if text_q < 0.6:
            comps.append("text_quality")
        if fatigue < 0.6:
            comps.append("fatigue")
        if behavior_recent < 0.6:
            comps.append("behavior")

        if not comps:
            comps.append("text_quality")  # default anchor

        return comps

    # ---------- Campaign-level generation ----------

    def _generate_for_campaign(
        self,
        campaign_name: str,
        terms: List[Dict[str, Any]],
        existing_messages: List[str],
        weak_components: List[str],
        variants_per_type: int = 3
    ) -> List[Dict[str, Any]]:
        """
        Generate creatives for a single campaign.
        variants_per_type applies to each variant_type (benefit, urgency, social_proof).
        """
        # Extract top tokens as "themes"
        top_terms = [t["term"] for t in terms][:10] if isinstance(terms, list) and terms else []
        if not top_terms:
            top_terms = ["comfort"]  # safe default in this domain

        # Build a corpus of existing text for overlap scoring
        existing_text = " ".join(existing_messages) if existing_messages else ""
        suggestions: List[Dict[str, Any]] = []

        # Decide which variant types to emphasize based on weak_components
        variant_types = ["benefit", "urgency", "social_proof"]

        idx = 1
        for v_type in variant_types:
            for i in range(variants_per_type):
                core_term = top_terms[i % len(top_terms)]

                headline, message, cta = self._compose_variant(
                    campaign_name=campaign_name,
                    core_term=core_term,
                    variant_type=v_type,
                    weak_components=weak_components
                )
                overlap_score = self._overlap_with_existing(headline + " " + message, existing_text)

                suggestion = {
                    "id": f"{campaign_name[:6].replace(' ', '')}-C{idx:03d}",
                    "headline": headline,
                    "message": message,
                    "cta": cta,
                    "variant_type": v_type,
                    "targeted_weakness": weak_components,
                    "core_term": core_term,
                    "overlap_score": float(overlap_score)
                }

                suggestions.append(suggestion)
                idx += 1

        # Optionally: shuffle suggestions to introduce variation
        random.shuffle(suggestions)
        return suggestions

    # ---------- Variant templates ----------

    def _compose_variant(
        self,
        campaign_name: str,
        core_term: str,
        variant_type: str,
        weak_components: List[str]
    ):
        """
        Template-based variant construction.
        """
        term_cap = core_term.capitalize()

        if variant_type == "benefit":
            if "text_quality" in weak_components:
                headline = f"{term_cap} comfort you can feel all day"
                message = (
                    f"Experience {core_term} underwear designed for a smooth, invisible fit under every outfit. "
                    "Soft, breathable fabric keeps you comfortable from morning to night."
                )
            else:
                headline = f"Upgrade your {core_term} basics"
                message = (
                    f"Step up your top drawer with {core_term} styles that feel good and look clean under clothes."
                )
            cta = "Shop comfort now"

        elif variant_type == "urgency":
            if "fatigue" in weak_components:
                headline = f"Fresh {term_cap} styles just dropped"
                message = (
                    f"Tired of the same old fit? Discover new {core_term} pieces designed for everyday comfort. "
                    "Limited-time launch pricing."
                )
            else:
                headline = f"{term_cap} sale ends soon"
                message = (
                    f"Stock up on your go-to {core_term} essentials with special pricing. "
                    "Don’t wait—popular sizes go first."
                )
            cta = "Grab your size today"

        else:  # social_proof
            headline = f"{term_cap} essentials customers keep re-ordering"
            message = (
                f"Join thousands who switched to our {core_term} underwear for a better fit and softer feel. "
                "Once you try them, you won’t go back."
            )
            cta = "See why they love it"

        return headline, message, cta

    # ---------- Overlap scoring ----------

    def _overlap_with_existing(self, text: str, existing_text: str) -> float:
        """
        Very simple overlap: Jaccard-like ratio of shared tokens.
        """
        def tokenize(t: str) -> List[str]:
            t = t.lower()
            t = re.sub(r"[^a-z0-9\s]", " ", t)
            return [tok for tok in t.split() if len(tok) > 2]

        new_tokens = set(tokenize(text))
        existing_tokens = set(tokenize(existing_text))
        if not new_tokens or not existing_tokens:
            return 0.0
        inter = len(new_tokens & existing_tokens)
        union = len(new_tokens | existing_tokens)
        return inter / float(union)
