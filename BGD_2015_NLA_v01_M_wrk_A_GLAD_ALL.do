*=========================================================================*
* GLOBAL LEARNING ASSESSMENT DATABASE (GLAD)
* Project information at: https://github.com/worldbank/GLAD
*
* Metadata to be stored as 'char' in the resulting dataset (do NOT use ";" here)
local region      = "BGD"
local year        = "2015"
local assessment  = "NLA"
local master      = "v01_M"
local adaptation  = "wrk_A_GLAD"
local module      = "ALL"
local ttl_info    = "Joao Pedro de Azevedo [eduanalytics@worldbank.org]"
local dofile_info = "last modified by Mina Ekramnia in October 21, 2019"
*
* Steps:
* 0) Program setup (identical for all assessments)
* 1) Open all rawdata, lower case vars, save in temp_dir
* 2) Combine all rawdata into a single file (merge and append)
* 3) Standardize variable names across all assessments
* 4) ESCS and other calculations
* 5) Bring WB countrycode & harmonization thresholds, and save dtas
*=========================================================================*

noisily {
 
  *---------------------------------------------------------------------------
  * 0) Program setup (identical for all assessments)
  *---------------------------------------------------------------------------

  // Parameters ***NEVER COMMIT CHANGES TO THOSE LINES!***
  //  - whether takes rawdata from datalibweb (==1) or from indir (!=1), global in 01_run.do
  local from_datalibweb = 1 // TODO: revert back to $from_datalibweb after problems with dlw are solved
  //  - whether checks first if file exists and attempts to skip this do file
  local overwrite_files = $overwrite_files
  //  - optional shortcut in datalibweb
  local shortcut = "$shortcut"
  //  - setting random seed at the beginning of each do for reproducibility
  set seed $master_seed

  // Set up folders in clone and define locals to be used in this do-file
  glad_local_folder_setup , r("`region'") y("`year'") as("`assessment'") ma("`master'") ad("`adaptation'")
  local temp_dir     "`r(temp_dir)'"
  local output_dir   "`r(output_dir)'"
  local surveyid     "`r(surveyid)'"
  local output_file  "`surveyid'_`adaptation'_`module'"

  // If user does not have access to datalibweb, point to raw microdata location
  if `from_datalibweb' == 1 {
    local input_dir	= "${input}/CNT/`region'/`region'_`year'_`assessment'/`surveyid'/Data/Stata"
  }

  // Confirm if the final GLAD file already exists in the local clone
  cap confirm file "`output_dir'/`output_file'.dta"
  // If the file does not exist or overwrite_files local is set to one, run the do
  if (_rc == 601) | (`overwrite_files') {

  // Filter the master country list to only this assessment-year
  *** SEGMENT NOT NEEDED FOR NLA, BY DEFAULT A SINGLE COUNTRY ***

    // Tokenized elements from the header to be passed as metadata
    local glad_description  "This dataset is part of the Global Learning Assessment Database (GLAD). It contains microdata from `assessment' `year'. Each observation corresponds to one learner (student or pupil), and the variables have been harmonized."
    local metadata          "region `region'; year `year'; assessment `assessment'; master `master'; adaptation `adaptation'; module `module'; ttl_info `ttl_info'; dofile_info `dofile_info'; description `glad_description'"

  *---------------------------------------------------------------------------
 * Prepare the thresholds for BGD
  *---------------------------------------------------------------------------

 * preserve
    
*      use "C:/Users/wb550776/Documents/GitHub/GLAD-Production/01_harmonization/011_rawdata/lp_thresholds_as_cpi.dta", clear
 *     local obs = _N+1
  *    set obs  `obs'
      
   *   replace surveyid = "BDG XXXX"
    *  replace idgrade = 5           if surveyid == "BDG XXXX"
     * replace lp01_threshold_var    if surveyid == "BDG XXXX"
      *replace lp01_threshold_val    if surveyid == "BDG XXXX"
      *replace lp01_threshold_res    if surveyid == "BDG XXXX"
    
   * restore
   
    *---------------------------------------------------------------------------
    * 1) Open all rawdata, lower case vars, save in temp_dir
    *---------------------------------------------------------------------------
    foreach prefix in NSA_2015_test_score_data  NSA_2015_Background_Information_of_HT_AT_and_students {
      if `from_datalibweb'==1 {
        noi edukit_datalibweb, d(country(`region') year(`year') type(EDURAW) surveyid(`surveyid') filename(`prefix'.dta) `shortcut')
      }
      else {
        use "`input_dir'/`prefix'.dta", clear
      }

      * Because the original dataset has 3 variables with the same info (school_name)
      * with only 1 of the 3 being filled in each observation, we aggregate them
      * for these similar variable names were breaking the rename *, lower line
      if "`prefix'" == "NSA_2015_Background_Information_of_HT_AT_and_students" {
        replace Schoolname = School_name if missing(Schoolname) & !missing(School_name)
        replace Schoolname = School_Name if missing(Schoolname) & !missing(School_Name)
        drop School_name School_Name
 
        * Because the sheet names will be used in filenames, replace any spaces
        replace original_sheet = subinstr(original_sheet," ","_",.)
     
      }

      rename *, lower

      * Studid ONLY exist in the TEST dataset, so we cant merge it to QUESTIONAIRE dataset
      * (also EMIS codes are not unique! why can't people use unique identifiers???)
      * Thus we attempt to build our own to serve as merge key
      
      * Sort and encode variables (sort is needed for the encoding to be consistent across dtas)
      foreach var_to_encode in sch_type sectionname {
        sort   `var_to_encode'
        encode `var_to_encode', gen(encoded_`var_to_encode')
      }
      
      * Destring components and string them back (because they should have trailing zeros)
      foreach var_to_destring in emis_code emis grade roll_no upazila_code {
        destring `var_to_destring', replace force
      }
      
      * This is our artificially created student id
      gen str9 str_emis_code = string(emis_code, "%09.0f")
      gen str3 str_roll_no   = string(roll_no,   "%03.0f")
      *gen artificial_studid  = str_emis_code + string(grade) + string(encoded_sectionname) + str_roll_no + string(encoded_sch_type)
      *gen artificial_studid  = str_emis_code + string(grade) + str_roll_no + string(encoded_sch_type)
      gen artificial_studid  = str_emis_code + string(grade) + str_roll_no 

      *gen EMIS_T = EMIS*100 + SType 

      compress
    save "`temp_dir'/`prefix'.dta", replace
    }

      noi disp as res "{phang}Step 1 completed (`output_file'){p_end}"

    *---------------------------------------------------------------------------
    * 2) Combine all rawdata into a single file (merge and append)
    *---------------------------------------------------------------------------

    * The questionnaire file is an appended dta of students, teachers and schools
    * so we break it first to ease the merges later
    foreach sheet in Students Subject_Teachers Head_Teachers {
      use "`temp_dir'/NSA_2015_Background_Information_of_HT_AT_and_students.dta", clear
      keep if original_sheet == "`sheet'"
      missings dropvars, force
      
      * Because the dataset has duplicates, will need some cleaning 
      if "`sheet'" == "Students" {
         *replace artificial_studid  = str_emis_code + string(grade) + string(encoded_sectionname) + str_roll_no + string(encoded_sch_type)
         duplicates drop artificial_studid, force
        ***** TODO1:  FIX HERE SO THAT this will be uniquely identified by artificial_studid
      }
      if "`sheet'" == "Head_Teachers" {
        ***** TODO2: FIX HERE SO THAT this will be uniquely identified by emis_code
         *replace emis_code
         *gen str9 str_emis_code = string(emis, "%09.0f") 
         *gen merge_id = EMIS + SType
         duplicates drop str_emis_code, force
         *duplicates drop emis, force
         *replace emis str_emis_code
      }
      
      save "`temp_dir'/`sheet'.dta", replace
    }

    use "`temp_dir'/NSA_2015_test_score_data.dta", clear    
    
    * For some reason, this dataset has duplicates!
    noi disp as txt "{phang2}Before cleaning duplicates, this dta had `=_N' observations{p_end}" 
    * Start with the easy case: delete observations that are fully identical
    duplicates drop
    * Then, delete when we have more than one score for a same studentid and studentname and subject
    duplicates drop artificial_studid subject, force
    noi disp as txt "{phang2}After cleaning duplicates, this dta had `=_N' observations{p_end}"     
    
    * Lastly, delete variables that would conflict with the reshape
    drop original_sheet         // variable we created when importing the original data from Excel
    drop schoolname studid      // hopefully already caught in artificial_studid
    drop sex form studentsname  // should come from questionnaire
    drop sectionname // should come from questionnaire
    drop encoded_sectionname // should come from questionnaire

    * Because scores for different subjects are presented in long format, reshape first
    reshape wide totsco totmax ptotsco ss100 ss300 encoded_sch_type sch_type  upazila, i(artificial_studid) j(subject) string
   
    * Now bring into the students scores, the student questionnaire info
    *merge 1:1 artificial_studid using "`temp_dir'/Students.dta", keep(master match using) nogen
    merge 1:1 artificial_studid using "`temp_dir'/Students.dta"

    * Plus brings in the school info
    merge m:1 str_emis_code using "`temp_dir'/Head_Teachers.dta", keep(master match using) nogen
   
    noi disp as res "{phang}Step 2 completed (`output_file'){p_end}"


    *---------------------------------------------------------------------------
    * 3) Harmonize variables across all assessments
    *---------------------------------------------------------------------------
    // For each variable class, we create a local with the variables in that class
    //     so that the final step of saving the GLAD dta  knows which vars to save

    // Every manipulation of variable must be enclosed by the ddi tags
    // Use clonevar instead of rename (preferable over generate)
    // The labels should be the same.
    // The generation of variables was commented out and should be replaced as needed

    // ID Vars:
    local idvars "idschool idgrade idclass idlearner"

    *<_idcntry_raw_>
    *gen idcntry_raw = "`region'"
    *label var idcntry_raw "Country ID, as coded in rawdata"
    *</_idcntry_raw_>

    *<_idschool_>
    clonevar idschool = emis_code
    destring idschool, replace
    replace idschool=99999 if idschool==.
    label var idschool "School ID"
    *</_idschool_>

    *<_idgrade_>
    clonevar  idgrade = grade
    replace idgrade= 99999 if idgrade==.
    label var idgrade "Grade ID"
    
    *<_idclass_>
    clonevar  idclass = sectionname
    replace idclass= "." if idclass==""
    label var idclass "Class ID"
    *</_idclass_>

    *<_idlearner_>
    clonevar  idlearner = artificial_studid
    label var idlearner "Learner ID"
    *</_idlearner_>
    
     // Drop any value labels of idvars, to be okay to append multiple surveys
   * foreach var of local idvars {
    *  label values `var' .
    *}

    *****PLACEHOLDER FROM HERE ON, YOU NEED TO CUSTOMIZE

    // VALUE Vars: 	  /* CHANGE HERE FOR YOUR ASSESSMENT!!! PIRLS EXAMPLE */
    local valuevars	"score_bgdnla* level_bgdnla*"

    *<_score_assessment_subject_>
    clonevar  score_bgdnla_read = ss100Bangla  //totscoBangla 0-42
    label var score_bgdnla_read "`assessment' score for reading"
    clonevar  score_bgdnla_math = ss100Math    //totscoMath 0-46
    label var score_bgdnla_math "`assessment' score for math"
    *</_score_assessment_subject_pv_>

    *<_level_assessment_subject_pv_>
    *Since the data does not provide a variable representing level of assessment we create that using this document: 
    *\\wbgfscifs01\GEDEDU\GDB\HLO_Database\CNT\BGD\BGD_2015_NLA\BGD_2015_NLA_v01_M\Doc\Reports
  
 * label define lblevels 0 "Below Level I" 1 "Level I" 2 "Level 2" 3 "Level 3" 4 "Level 4" .a "Missing test score"
		*forvalues pv = 1/5 {
			*gen level_pasec_read_0`pv': lblevels = .a
			*gen level_pasec_math_0`pv': lblevels = .a
      *</_level_assessment_subject_pv_>
      
			// Math
			gen level_bgdnla_math = 5 if ss100Math < 145 & ss100Math >= 124  
			replace level_bgdnla_math = 4 if ss100Math < 124 & ss100Math >= 113
			replace level_bgdnla_math = 3 if ss100Math < 112 & ss100Math >= 101
			replace level_bgdnla_math = 2 if ss100Math < 101 & ss100Math >= 90
			replace level_bgdnla_math = 1 if ss100Math < 90
      label var level_bgdnla_math "`assessment' level for math"
      
			// Reading, Subject = Bangla
      gen level_bgdnla_read = 5 if ss100Bangla < 138 & ss100Bangla >= 122
			replace level_bgdnla_read = 4 if ss100Bangla < 122 & ss100Bangla >= 108
			replace level_bgdnla_read = 3 if ss100Bangla < 108 & ss100Bangla >= 95
			replace level_bgdnla_read = 2 if ss100Bangla < 95 & ss100Bangla >= 85
			replace level_bgdnla_read = 1 if ss100Bangla < 85
      label var level_bgdnla_read "`assessment' level for reading"


    // TRAIT Vars:
    local traitvars	"age urban* male escs"

    *<_age_>
    replace age = item5
    label var age "Learner age at time of assessment"
    *</_age_>

    *<_urban_>
    gen byte urban =  . // No rural data in 2015 according to 
    label var urban "School is located in urban/rural area"
    *</_urban_>

    *<_urban_o_> //the data does not include urban information 
    //decode acbg05a, g(urban_o)
    //label var urban_o "Original variable of urban: population size of the school area"
    *</_urban_o_>

    *<_male_> 
    destring sex, replace
    gen male = (sex == 1) & !missing(sex)
    label var male "Learner gender is male/female"
    *</_male_>


    // SAMPLE Vars:
    local samplevars "learner_weight"
    //No weight variable, some dataset had and others didn't. 

    *<_learner_weight_> //We did not use the weight variable, partly because some dataset had and others didn't
    gen learner_weight  = .
    label var learner_weight "Total learner weight"
    *</_learner_weight_>


    noi disp as res "{phang}Step 3 completed (`output_file'){p_end}"


    *---------------------------------------------------------------------------
    * 4) ESCS and other calculations
    *---------------------------------------------------------------------------

    // Placeholder for other operations that we may want to include (kept in ALL-BASE)
    *<_escs_>
    * code for ESCS
    * Economic Status by Dwelling, Assets, based on NSA 2015 report 
    * “Puccas” are concrete dwellings, “Semi-puccas” are brick wall homes, usually with tin roofs. “kucchas” : bamboo, wood, etc.
    *The difference in Bangla scores between those students living in Pucca’s or Semi-Pucca’s was not statistically significant. 
    *However, the difference between students living in Kuccha’s and Pucca’s and Semi-Pucca’s was statistically significant. 
    
    destring item21, replace
    gen pucca = ( item21 == 1)
    replace pucca = . if  ( item21 == .)
    
    gen kachha = ( item21 == 3)
    replace kachha = . if  ( item21 == .)

    destring item20a- item20i, replace
    foreach var of varlist item20a- item20i {
      replace `var' = 0 if `var' == 2
    }

    *wealth index based on BD Edu Section Review 2014
    *Based on “Bangladesh Education Sector Review: Seeding Fertile Ground—Education That Works for Bangladesh.” Dhaka: World Bank.)
    egen escs = rsum(pucca kachha item20b item20c item20d item20e item20f)
    * label for ESCS
    label var escs "Wealth Index"
    tab escs

    *</_escs_>
    noi disp as res "{phang}Step 4 completed (`output_file'){p_end}"

    *---------------------------------------------------------------------------
    * 5) Bring WB countrycode and save GLAD and GLAD_BASE dta
    *---------------------------------------------------------------------------

    * Surveyid is needed to merge harmonization proficiency thresholds
    gen str surveyid = "`region'_`year'_`assessment'"
    label var surveyid "Survey ID (Region_Year_Assessment)"    
      
    * Because this is an NLA, we don't need to merge with the master country list
    * we just define it promptly
    gen countrycode = "`region'"
    gen national_level = 1
    local keyvars "surveyid countrycode national_level"
   

    // This function compresses the dataset, adds metadata passed in the arguments as chars, save GLAD_BASE.dta
    // which contains all variables, then keep only specified vars and saves GLAD.dta, and delete files in temp_dir
    edukit_save,  filename("`output_file'") path("`output_dir'") dir2delete("`temp_dir'")              ///
                idvars("`idvars'") varc("key `keyvars'; value `valuevars'; trait `traitvars'; sample `samplevars'") ///
                metadata("`metadata'") collection("GLAD")

    noi disp as res "Creation of `output_file'.dta completed"

  }

  else {
    noi disp as txt "Skipped creation of `output_file'.dta (already found in clone)"
    // Still loads it, to generate documentation
    use "`output_dir'/`output_file'.dta", clear
  }
}
