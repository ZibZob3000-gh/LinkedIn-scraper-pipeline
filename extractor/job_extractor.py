class JobExtractor:
    def __init__(self, skills_module, industry_module, skills_data, llm_config, prompts):
        self.skills_module = skills_module
        self.industry_module = industry_module
        self.llm_config = llm_config
        self.prompts = prompts

        # ---- Skill taxonomy & synonyms ----
        self.skill_to_info = {
            base_skill.lower(): info
            for base_skill, info in skills_data["taxonomy"].items()
        }

        self.skill_synonyms = {
            base_skill.lower(): [syn.lower() for syn in synonyms]
            for base_skill, synonyms in skills_data["synonyms"].items()
        }

        self.fuzzy_threshold = skills_data.get("fuzzy_threshold", 85)

        # Flatten synonym dict for exact matching: synonym -> base skill
        self.synonym_to_skill = {
            syn: base_skill
            for base_skill, synonyms in self.skill_synonyms.items()
            for syn in synonyms
        }

        # Prepare fuzzy choices
        self.all_synonyms = list(self.synonym_to_skill.keys())

        # Map wrapper
        def map_skill_wrapper(extracted_skill: str):
            return self.skills_module.map_skill_with_synonyms_verbose(
                extracted_skill=extracted_skill,
                synonym_to_skill=self.synonym_to_skill,
                skill_lookup=self.skill_to_info,
                all_synonyms=self.all_synonyms,
                fuzzy_threshold=self.fuzzy_threshold
            )

        self.map_skill_with_synonyms_verbose = map_skill_wrapper

        # Columns to preserve from original record
        self.original_columns = [
            "id", "title", "job_function", "job_level", "job_type",
            "is_remote", "location", "date_posted", "job_url", "site",
            "company", "company_url", "company_industry", "description"
        ]

    def process_job(self, job_record):
        raw_desc = job_record.get("description", "")
        description = self.skills_module.clean_job_description(raw_desc)

        # ---- Step 1: Extract skills & languages ----
        skills_data = self.skills_module.extract_skills(
            description,
            llm_config=self.llm_config,
            prompts=self.prompts
        )
        hard_skills = [s.strip() for s in skills_data.get("hard_skills", []) if s.strip()]
        spoken_languages = [s.strip() for s in skills_data.get("spoken_languages", []) if s.strip()]

        # ---- Step 2: Estimate levels & contacts ----
        extra_data = self.skills_module.estimate_skill_levels(
            description,
            hard_skills,
            spoken_languages=spoken_languages,
            llm_config=self.llm_config,
            prompts=self.prompts
        )
        skill_levels = extra_data["skill_levels"]
        spoken_languages_levels = extra_data["spoken_languages_levels"]
        contact_details = extra_data["contact_details"]
        language_description = extra_data["language_description"]

        # ---- Step 3: Map hard skills to taxonomy IDs ----
        skill_levels_with_ids = {}
        for skill_name, level in skill_levels.items():
            mapped = self.map_skill_with_synonyms_verbose(skill_name)
            if mapped["ID"]:
                skill_levels_with_ids[str(mapped["ID"])] = level
            else:
                skill_levels_with_ids[skill_name] = level  # fallback to skill name

        # ---- Step 4: Industry mapping ----
        company_industry = job_record.get("company_industry", "")
        industry_mapping = self.industry_module.map_industry(
            company_industry,
            llm_config=self.llm_config
        )
        main_ind = industry_mapping.get("main_industry") or "unknown"
        sub_ind = industry_mapping.get("subindustry") or "unknown"
        company_industry_str = f"{main_ind}: {sub_ind}"

        # ---- Step 5: Merge original columns with enriched columns ----
        final_record = {col: job_record.get(col) for col in self.original_columns}
        final_record.update({
            "hard_skills": skill_levels_with_ids,
            "spoken_languages": spoken_languages_levels,
            "contact_details": contact_details,
            "language_description": language_description,
            "company_industry": company_industry_str,
            "description": description
        })

        return final_record
