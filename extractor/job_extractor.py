class JobExtractor:
    def __init__(self, skills_module, skills_data, llm_config, prompts, industry_mapping):
        self.skills_module = skills_module
        self.llm_config = llm_config
        self.prompts = prompts
        self.industry_mapping = industry_mapping

        # ==== Taxonomy Data ====
        self.skill_to_info = {
            base_skill.lower(): info
            for base_skill, info in skills_data["taxonomy"].items()
        }
        self.skill_synonyms = {
            base_skill.lower(): [syn.lower() for syn in synonyms]
            for base_skill, synonyms in skills_data["synonyms"].items()
        }
        self.synonym_to_skill = {
            syn: base_skill
            for base_skill, synonyms in self.skill_synonyms.items()
            for syn in synonyms
        }
        self.all_synonyms = list(self.synonym_to_skill.keys())
        self.fuzzy_threshold = skills_data.get("fuzzy_threshold", 85)

        # Column preservation
        self.original_columns = [
            "id", "title", "job_function", "job_level", "job_type",
            "is_remote", "location", "date_posted", "job_url", "site",
            "company", "company_url", "company_industry", "description"
        ]

    def process_job(self, job_record):
        desc_raw = job_record.get("description", "")
        description = self.skills_module.clean_job_description(desc_raw)

        # === Step 1: Extract hard + soft skills ===
        skills_data = self.skills_module.extract_skills(description, self.llm_config, self.prompts)
        hard_skills = [s.strip() for s in skills_data.get("hard_skills", []) if s.strip()]
        soft_skills = [s.strip() for s in skills_data.get("soft_skills", []) if s.strip()]

        # === Step 2: Extract spoken languages + dept + contact ===
        meta_data = self.skills_module.extract_metadata(description, self.llm_config, self.prompts)
        spoken_languages = meta_data["spoken_languages"]
        department = meta_data["department"]
        contact_details = meta_data["contact_details"]
        language_description = meta_data["language_description"]

        # === Step 3: Extract levels ===
        level_data = self.skills_module.extract_skill_levels(
            hard_skills, spoken_languages, description, self.llm_config, self.prompts
        )
        hard_skill_levels = level_data["hard_skill_levels"]
        spoken_languages_levels = level_data["spoken_languages_levels"]

        # === Step 4: Industry Mapping ===
        company_industry = job_record.get("company_industry", "")
        industry_mapping_result = self.skills_module.map_industry(
            company_industry,
            llm_config=self.llm_config,
            industry_mapping=self.industry_mapping,
            prompts=self.prompts
        )
        main_ind = industry_mapping_result.get("main_industry", "unknown")
        sub_ind = industry_mapping_result.get("subindustry", "unknown")
        company_industry_str = f"{main_ind}: {sub_ind}"

        # === Step 5: Split hard skills into Acumen/Other ===
        hard_skills_acumen = {}
        hard_skills_other = {}

        for skill_name, level in hard_skill_levels.items():
            mapped = self.skills_module.map_skill_with_synonyms_verbose(
                extracted_skill=skill_name,
                synonym_to_skill=self.synonym_to_skill,
                skill_lookup=self.skill_to_info,
                all_synonyms=self.all_synonyms,
                fuzzy_threshold=self.fuzzy_threshold
            )
            if mapped["ID"]:
                hard_skills_acumen[str(mapped["ID"])] = level
            else:
                hard_skills_other[skill_name] = level

        # === Step 6: Final Output Record ===
        final = {col: job_record.get(col) for col in self.original_columns}
        final.update({
            "hard_skills_acumen": hard_skills_acumen,
            "hard_skills_other": hard_skills_other,
            "soft_skills": soft_skills,
            "spoken_languages": spoken_languages_levels,
            "department": department,
            "contact_details": contact_details,
            "language_description": language_description,
            "company_industry": company_industry_str,
            "description": description
        })
        return final
