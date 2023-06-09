*=========================================================================*
* GLOBAL LEARNING ASSESSMENT DATABASE (GLAD)
* Project information at: https://github.com/worldbank/GLAD
*
* Metadata to be stored as 'char' in the resulting dataset (do NOT use ";" here)
local region      = "AFG"   /* LAC, SSA, WLD or CNT such as KHM RWA */
local year        = "2013"  /* 2015 */
local assessment  = "NLA" /* PIRLS, PISA, EGRA, etc */
local master      = "v01_M" /* usually v01_M, unless the master (eduraw) was updated*/
local adaptation  = "wrk_A_GLAD" /* no need to change here */
local module      = "ALL"  /* for now, we are only generating ALL and ALL-BASE in GLAD */
local ttl_info    = "Joao Pedro de Azevedo [eduanalytics@worldbank.org]" /* no need to change here */
local dofile_info = "last modified by Mina Ekramnia in February 24, 2020"
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
  local from_datalibweb = 1 //$from_datalibweb
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
  if `from_datalibweb' == 0 {
    local input_dir	= "${input}/`region'/`region'_`year'_`assessment'/`surveyid'/Data/Stata"
  }

  // Confirm if the final GLAD file already exists in the local clone
  cap confirm file "`output_dir'/`output_file'.dta"
  // If the file does not exist or overwrite_files local is set to one, run the do
  if (_rc == 601) | (`overwrite_files') {

    // Tokenized elements from the header to be passed as metadata
    local glad_description  "This dataset is part of the Global Learning Assessment Database (GLAD). It contains microdata from `assessment' `year'. Each observation corresponds to one learner (student or pupil), and the variables have been harmonized."
    local metadata          "region `region'; year `year'; assessment `assessment'; master `master'; adaptation `adaptation'; module `module'; ttl_info `ttl_info'; dofile_info `dofile_info'; description `glad_description'"

    *---------------------------------------------------------------------------
    * 1) Open all rawdata, lower case vars, save in temp_dir
    *---------------------------------------------------------------------------

    /* NOTE: Some assessments will loop over `prefix'`cnt' (such as PIRLS, TIMSS),
       then create a temp file with all prefixs of a cnt merged.
       but other asssessments only need to loop over prefix (such as LLECE).
       See the two examples below and change according to your needs */

    foreach prefix in AFG_G6_DATABASE_replicates AFG_G6_cleaned_school_data_with_student_IDs {
      // Temporary copies of the 2 rawdatasets needed for each country (new section)
        if `from_datalibweb'== 1{
          noi edukit_datalibweb, d(country(`region') year(`year') type(EDURAW) surveyid(`surveyid') filename(`prefix'`cnt'.dta) `shortcut')
        }
        else {
          use "`input_dir'/`prefix'.dta", clear
        }
         rename *, lower
         compress
         save "`temp_dir'/`prefix'.dta", replace
       }


    noi disp as res "{phang}Step 1 completed (`output_file'){p_end}"

    *---------------------------------------------------------------------------
    * 2) Combine all rawdata into a single file (merge and append)
    *---------------------------------------------------------------------------

     // Merge the 2 rawdatasets into a single TEMP country file
    use "`temp_dir'/AFG_G6_DATABASE_replicates.dta", clear
    merge 1:1 student_id using "`temp_dir'/AFG_G6_cleaned_school_data_with_student_IDs.dta", keep(master match) nogen
    gen stidsch = substr(student_id, 1, 5)
    gen trgrade = 6

    noi disp as res "{phang}Step 2 completed (`output_file'){p_end}"

    *---------------------------------------------------------------------------
    * 3) Standardize variable names across all assessments
    *---------------------------------------------------------------------------
    // For each variable class, we create a local with the variables in that class
    //     so that the final step of saving the GLAD dta  knows which vars to save

    // Every manipulation of variable must be enclosed by the ddi tags
    // Use clonevar instead of rename (preferable over generate)
    // The labels should be the same.
    // The generation of variables was commented out and should be replaced as needed

    // ID Vars:
    local idvars "idschool idgrade idlearner"

    *<_idschool_>
    clonevar idschool = schoolid
    destring idschool, replace
    replace idschool=99999 if idschool==.
    label var idschool "School ID"
    *</_idschool_>

    *<_idgrade_>  //grade 3 and 4 combined
    gen idgrade= trgrade //mentioned STF Grade in the data
    label var idgrade "Grade ID"
    *</_idgrade_>

    *<_idlearner_>
    clonevar idlearner = student_id //student_id in MTEG
    destring idlearner, replace
    replace idlearner=99999 if idlearner==.
    label var idlearner "Learner ID"
    *</_idlearner_>

    // VALUE Vars: 	 **Use https://research.acer.edu.au/cgi/viewcontent.cgi?article=1017&context=mteg page 20 for Math
    local valuevars	"score_nla* level_nla*"

    *<_score_assessment_subject_pv_>
    *foreach pv in 01 02 03 04 05 {
      *clonevar score_pirls_read_`pv' = asrrea`pv'
    forvalues pv = 1/5 {
    clonevar  score_nla_read_0`pv' = read_pv`pv' //NLAs- Labels
    label var score_nla_read_0`pv' "Plausible value `pv': `assessment' score for read"
    char      score_nla_read_0`pv'[clo_marker] "number"
    clonevar  score_nla_math_0`pv' = math_pv`pv'
    label var score_nla_math_0`pv' "Plausible value `pv': `assessment' score for math"
    char      score_nla_math_0`pv'[clo_marker] "number"
  }

    *<_level_assessment_subject_pv_>
  // Data does not contain a variable for levels, but the documentation provides this conversion
  // for details: https://research.acer.edu.au/mteg/17/
  label define lblevels 4 "Below Level 4" 5 "Level 5" 6 "Level 6" 7 "Level 7"  8 "Level 8" 9 "Level 9" 10 "Level 10" 11 "Level 11"  .a "Missing test score"
  forvalues pv = 1/5 {
  gen level_nla_read_0`pv': lblevels = .a
  gen level_nla_math_0`pv': lblevels = .a
      //Proficiency levels are universal for all grades, according to the report.
  // Reading, grade 3-4
  replace level_nla_read_0`pv' = 11 if read_pv`pv'>=234 & !missing(read_pv`pv')
  replace level_nla_read_0`pv' = 10 if read_pv`pv'>=222 & read_pv`pv'<234
  replace level_nla_read_0`pv' = 9 if read_pv`pv'>=210 & read_pv`pv'<222
  replace level_nla_read_0`pv' = 8 if read_pv`pv'>=198 & read_pv`pv'<210
  replace level_nla_read_0`pv' = 7 if read_pv`pv'>=186 & read_pv`pv'<198
  replace level_nla_read_0`pv' = 6 if read_pv`pv'>=174 & read_pv`pv'<186
  replace level_nla_read_0`pv' = 5 if read_pv`pv'>=162 & read_pv`pv'<174
  replace level_nla_read_0`pv' = 4 if read_pv`pv'<162

  // Mathematics, grade 3-4
  replace level_nla_math_0`pv' = 11 if math_pv`pv'>=259 & !missing(math_pv`pv')
  replace level_nla_math_0`pv' = 10 if math_pv`pv'>=242 & math_pv`pv'<259
  replace level_nla_math_0`pv' = 9 if math_pv`pv'>=226 & math_pv`pv'<242
  replace level_nla_math_0`pv' = 8 if math_pv`pv'>=210 & math_pv`pv'<226
  replace level_nla_math_0`pv' = 7 if math_pv`pv'>=194 & math_pv`pv'<210
  replace level_nla_math_0`pv' = 6 if math_pv`pv'>=178 & math_pv`pv'<194
  replace level_nla_math_0`pv' = 5 if math_pv`pv'<178
  label var level_nla_read_0`pv' "Plausible value `pv': `assessment' level for read"
  label var level_nla_math_0`pv' "Plausible value `pv': `assessment' level for math"
  char      level_nla_read_0`pv'[clo_marker] "factor"
  char      level_nla_math_0`pv'[clo_marker] "factor"
  }

    // TRAIT Vars:
    local traitvars	"age urban* male escs"

    *<_age_>
    destring sc02q01, replace
    gen age = sc02q01 if  !missing(sc02q01)	& sc02q01!= 99 & sc02q01!=97
    label var age "Learner age at time of assessment"
    *</_age_>

    *<_urban_>
    gen byte urban = ruralurban
    label var urban "School is located in Rural/Urban area"
    *</_urban_>

    *<_male_> 
    gen male = (gender == "2") & !missing(gender)
    label var male "Learner gender is male/female"
    *</_male_>

    *<_escs_>
    gen byte escs = .  // No escs var included
    label var escs "No data available for StudentSocio-Economic Status"
    *</_escs_>

    // SAMPLE Vars:
    local samplevars "learner_weight"

    *<_learner_weight_>
    clonevar learner_weight  = st_weight
    label var learner_weight "Learner weight not included"
    *</_learner_weight_>

    *<_weight_replicateN_>
    forvalues i=1(1)110 {
      clonevar  weight_replicate`i' = st_wr`i'
      label var weight_replicate`i' "Replicate weight `i'"
  }
  *</_weight_replicateN_>

    noi disp as res "{phang}Step 3 completed (`output_file'){p_end}"

    *---------------------------------------------------------------------------
    * 4) ESCS and other calculations
    *---------------------------------------------------------------------------
    // Maurice (contact person): Unfortunately we did not have the resources to do our own exploration around SES.

    noi disp as res "{phang}Step 4 completed (`output_file'){p_end}"

    *---------------------------------------------------------------------------
    * 5) Bring WB countrycode & harmonization thresholds, and save dtas
    *---------------------------------------------------------------------------

    // Brings World Bank countrycode from ccc_list
    // NOTE: the *assert* is intentional, please do not remove it.
    // if you run into an assert error, edit the 011_rawdata/master_countrycode_list.csv

    * Surveyid is needed to merge harmonizatio  n proficiency thresholds
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
