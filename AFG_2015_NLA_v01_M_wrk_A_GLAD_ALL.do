*=========================================================================*
* GLOBAL LEARNING ASSESSMENT DATABASE (GLAD)
* Project information at: https://github.com/worldbank/GLAD
*
* Metadata to be stored as 'char' in the resulting dataset (do NOT use ";" here)
local region      = "AFG"   /* LAC, SSA, WLD or CNT such as KHM RWA */
local year        = "2015"  /* 2015 */
local assessment  = "NLA" /* PIRLS, PISA, EGRA, etc */
local master      = "v01_M" /* usually v01_M, unless the master (eduraw) was updated*/
local adaptation  = "wrk_A_GLAD" /* no need to change here */
local module      = "ALL"  /* for now, we are only generating ALL and ALL-BASE in GLAD */
local ttl_info    = "Joao Pedro de Azevedo [eduanalytics@worldbank.org]" /* no need to change here */
local dofile_info = "last modified by Mina Ekramnia in November 26, 2019"  /* change date*/
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
  local from_datalibweb = 0 //$from_datalibweb
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

  /*
    // Filter the master country list to only this assessment-year
    use "${clone}/01_harmonization/011_rawdata/master_countrycode_list.dta", clear
    keep if (assessment == "`assessment'") & (year == `year')
    // Most assessments use the numeric idcntry_raw but a few (ie: PASEC 1996) have instead idcntry_raw_str
    if use_idcntry_raw_str[1] == 1 {
      drop   idcntry_raw
      rename idcntry_raw_str idcntry_raw
    }
    keep idcntry_raw national_level countrycode
    save "`temp_dir'/countrycode_list.dta", replace
*/

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

     foreach prefix in AFG_G3_MS_Database_20171011 G3_SCQ_MS_Database_20170922{
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
       
        *gen str9 str_stidsch = string(stidsch, "%09.0f")
        *gen str9 str_stidstd = string(stidstd, "%09.0f")
        *save "`temp_dir/`prefix'.dta",replace
  *  save "`temp_dir'/TEMP_`surveyid'.dta", replace

    noi disp as res "{phang}Step 1 completed (`output_file'){p_end}"


    *---------------------------------------------------------------------------
    * 2) Combine all rawdata into a single file (merge and append)  
    *---------------------------------------------------------------------------
    
 *    use "`temp_dir'/AFG_G3_MS_Database_Final_20171011.dta", clear 
        // Merge the 2 rawdatasets into a single TEMP country file
      use "`temp_dir'/AFG_G3_MS_Database_20171011.dta", clear
      merge m:1 stidsch using "`temp_dir'/G3_SCQ_MS_Database_20170922.dta", keep(master match) nogen
     
       *gen artificial_studid  = stidstd + stidsch
    /* NOTE: the merge / append of all rawdata saved in temp in above step
       will vary slightly by assessment.
       See the two examples continuedw and change according to your needs */


    /***** BEGIN PIRLS 2011 EXAMPLE *****

    fs "`temp_dir'/TEMP_`surveyid'_p_*.dta"
    local firstfile: word 1 of `r(files)'
    use "`temp_dir'/`firstfile'", clear
    foreach f in `r(files)' {
      if "`f'" != "`firstfile'" append using "`temp_dir'/`f'"
    }

    ***** END OF PIRLS 2011 EXAMPLE  *****/

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
* idclass

    *<_idcntry_raw_>
 * clonevar idcntry_raw =  "`region'"
 * label var idcntry_raw "Country ID, as coded in rawdata"
    *</_idcntry_raw_>

    *<_idschool_>
    clonevar idschool = stidsch
    destring idschool, replace
    replace idschool=99999 if idschool==.
    label var idschool "School ID"
    *</_idschool_>

    *<_idgrade_>  //grade 3 and 4 combined
    destring trgrade, replace 
    gen idgrade = trgrade //mentioned STF Grade in the data 
    replace idgrade= 3 if idgrade==4 //always report the grades combined  
   *destring idgrade, replace
    label var idgrade "Grade ID"
    *</_idgrade_>

    *<_idclass_> //READ DOCUMENTATION
   * clonevar idclass = 
*  replace idclass= "." if idclass==""
    *label var idclass "Class ID"
    *</_idclass_>

    *<_idlearner_>
    clonevar idlearner = stidstd //student_id in MTEG
    destring idlearner, replace
    replace idlearner=99999 if idlearner==.
    label var idlearner "Learner ID"
    *</_idlearner_>

    // Drop any value labels of idvars, to be okay to append multiple surveys
  *  foreach var of local idvars {
   *   label values `var' .
    *}

    // VALUE Vars: 	 **Use https://research.acer.edu.au/cgi/viewcontent.cgi?article=1017&context=mteg page 20 for Math
    local valuevars	"score_nla* level_nla*"

    *<_score_assessment_subject_pv_>
    *foreach pv in 01 02 03 04 05 {
      *clonevar score_pirls_read_`pv' = asrrea`pv'
    forvalues pv = 1/5 {
			clonevar  score_nla_read_0`pv' = readpv`pv' //NLAs- Labels
			label var score_nla_read_0`pv' "Plausible value `pv': `assessment' score for read"
			char      score_nla_read_0`pv'[clo_marker] "number"
			clonevar  score_nla_math_0`pv' = mathpv`pv'
			label var score_nla_math_0`pv' "Plausible value `pv': `assessment' score for math"
			char      score_nla_math_0`pv'[clo_marker] "number"
		}
    *</_score_assessment_subject_pv_>

    *<_level_assessment_subject_pv_>
		// Data does not contain a variable for levels, but the documentation provides this conversion
		// for details: https://research.acer.edu.au/mteg/17/
		label define lblevels 3 "Below Level 3" 4 "Level 4" 5 "Level 5" 6 "Level 6" 7 "Level 7"  8 "Level 8" 9 "Level 9" 10 "Level 10" .a "Missing test score"
		forvalues pv = 1/5 {
			gen level_nla_read_0`pv': lblevels = .a
			gen level_nla_math_0`pv': lblevels = .a
      //Proficiency levels are universal for all grades, according to the report. 
			// Reading, grade 3-4
      replace level_nla_read_0`pv' = 10 if readpv`pv'>=222 & !missing(readpv`pv')
      replace level_nla_read_0`pv' = 9 if readpv`pv'>=210 & readpv`pv'<222 
			replace level_nla_read_0`pv' = 8 if readpv`pv'>=198 & readpv`pv'<210 
			replace level_nla_read_0`pv' = 7 if readpv`pv'>=186 & readpv`pv'<198 
      replace level_nla_read_0`pv' = 6 if readpv`pv'>=174 & readpv`pv'<186 
			replace level_nla_read_0`pv' = 5 if readpv`pv'>=162 & readpv`pv'<174 
			replace level_nla_read_0`pv' = 4 if readpv`pv'<162 
			// Mathematics, grade 3-4
			replace level_nla_math_0`pv' = 9 if mathpv`pv'>=226 & !missing(mathpv`pv')
			replace level_nla_math_0`pv' = 8 if mathpv`pv'>=210 & mathpv`pv'<226 
			replace level_nla_math_0`pv' = 7 if mathpv`pv'>=194 & mathpv`pv'<210 
      replace level_nla_math_0`pv' = 6 if mathpv`pv'>=178 & mathpv`pv'<194 
	    replace level_nla_math_0`pv' = 5 if mathpv`pv'>=162 & mathpv`pv'<178 
			replace level_nla_math_0`pv' = 4 if mathpv`pv'>=148 & mathpv`pv'<162 
			replace level_nla_math_0`pv' = 3 if mathpv`pv'<148  
		  label var level_nla_read_0`pv' "Plausible value `pv': `assessment' level for read"
      label var level_nla_math_0`pv' "Plausible value `pv': `assessment' level for math"
		  char      level_nla_read_0`pv'[clo_marker] "factor"
		  char      level_nla_math_0`pv'[clo_marker] "factor" 
		}
   
    *<_score_assessment_subject_pv_> //from SSA_1996_PASEC try to fix for AFG

    // TRAIT Vars:
    local traitvars	"age urban* male escs"

    *<_age_>
    gen age = trage if  !missing(trage)	& trage!= 99
    label var age "Learner age at time of assessment"
    *</_age_>

    *<_urban_>
    destring sc10q01, replace
    gen byte urban = 1 if sc10q01==1 //remote
    replace urban =2 if sc10q01 ==2  //rural
    replace urban =3 if sc10q01 ==3  //in or near small town
    replace urban =4 if sc10q01 ==4  //in or near large town or city 
    label var urban "School is located in Remote/Rural/In or near small town/Urban: In or near city area"
    *</_urban_>

    *<_urban_o_>
    *decode acbg05a, g(urban_o)
    *abel var urban_o "Original variable of urban: population size of the school area"
    *</_urban_o_>

    *<_male_>
    destring trgender, replace
    gen  male = (trgender == 2) & !missing(trgender)
    label var male "Learner gender is male/female"
    *</_male_>
    
      *<_escs_>
    gen byte escs = .  // No escs var included
    label var escs "No data available for StudentSocio-Economic Status"
    *</_escs_>

    // SAMPLE Vars:		 
*   local samplevars "learner_weight jkzone jkrep"
    local samplevars "jkzone jkind weight_replicate*"
    *strata

    *<_learner_weight_>
    gen learner_weight = 1
    *clonevar learner_weight  = totwgt
    label var learner_weight "Learner weight not included"
    *</_learner_weight_>

    *<_jkzone_>
    label var jkzone "Jackknife zone"
    *</_jkzone_>

    *<_jkrep_>
  * label var jkrep "Jackknife replicate code"
    *</_jkrep_>
    
      *<_jkind_>
    label var jkind "Jackknife index indicator"
    *</_jkind_>
    
	  *<_weight_replicateN_>
		forvalues i=1(1)85 {
			clonevar  weight_replicate`i' = rwgt`i'
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



*    // New variable class: keyvars (not IDs, but rather key to describe the dataset)
*    local keyvars "surveyid countrycode national_level"

*    // Harmonization of proficiency on-the-fly, based on thresholds as CPI
*   glad_hpro_as_cpi
*    local thresholdvars "`r(thresholdvars)'"
*    local resultvars    "`r(resultvars)'"

*    // Update valuevars to include newly created harmonized vars (from the ado)
*    local valuevars : list valuevars | resultvars



