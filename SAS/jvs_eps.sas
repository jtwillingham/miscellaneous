/*** Job Vacancy Survey Estimation Production System ***/

Options NoCenter NoDate NoNumber SymbolGen MLogic MPrint;

%let inputlocation = /oes3s8/jvs/production/&_RMTUSER./input;
%let outputlocation = /oes3s8/jvs/production/&_RMTUSER./output;

Libname PermData "&inputlocation";
Libname PermEst "&outputlocation";

/****************************\
Beginning of JVS Importation
\****************************/

%Macro Optional;
	%If &Occ_Emp^=' ' %Then %Do;
		Data Occ_Emp;
			infile &Occ_Emp delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2;
			format Est_Geo $4.;
			format Est_Ind $6.;
			format Est_Size $1.;
			format Est_Own $1.;
			format Est_Occ $7.;
			informat Est_Geo $4.;
			informat Est_Ind $6.;
			informat Est_Size $1.;
		informat Est_Own $1.;
		informat Est_Occ $7.;
		input Est_Geo $
		      Est_Ind $
		      Est_Size $
		      Est_Own $
		      Est_Occ $
		      OccEmp_Est;
		Run;
		Proc Contents NoPrint Data=Occ_Emp Out=Contents_Occ_Emp; Run;
		Data PermData.Occ_Emp; Set Occ_Emp; Run;
		Data Contents_Occ_Emp (Keep=MemName NObs);
			Set Contents_Occ_Emp (In=In1);
				If In1 Then Type=1;
				If Lag(Type)=Type Then Delete;
		Run;
		Title "OCCUPATIONAL EMPLOYMENT DATA: Record Count";
		Proc Print NoObs Data=Contents_Occ_Emp; Run;
	%End;
	%If &Occ_Emp=' ' %Then %Do;
		Data PermData.Occ_Emp;
			Est_Geo='9999';
			Est_Ind='999999';
			Est_Size='0';
			Est_Own='0';
			Est_Occ='99-9999';
			OccEmp_Est=1;
		Run;
	%End;
	%If &TR_Data^=' ' %Then %Do;
		Data TR_Data;
			infile &TR_Data delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2;
			format Est_Geo $4.;
			format Est_Ind $6.;
			format Est_Size $1.;
			format Est_Own $1.;
			format Est_Occ $7.;
			informat Est_Geo $4.;
			informat Est_Ind $6.;
			informat Est_Size $1.;
			informat Est_Own $1.;
			informat Est_Occ $7.;
			input Est_Geo $
			      Est_Ind $
			      Est_Size $
			      Est_Own $
			      Est_Occ $
			      T_Rate;
		Run;
		Proc Contents NoPrint Data=TR_Data Out=Contents_TR_Data; Run;
		Data PermData.TR_Data; Set TR_Data; Run;
		Data Contents_TR_Data (Keep=MemName NObs);
			Set Contents_TR_Data (In=In1);
				If In1 Then Type=1;
				If Lag(Type)=Type Then Delete;
		Run;
		Title "TURNOVER RATE DATA: Record Count";
		Proc Print NoObs Data=Contents_TR_Data; Run;
	%End;
	%If &TR_Data=' ' %Then %Do;
		Data PermData.TR_Data;
			Est_Geo='9999';
			Est_Ind='999999';
			Est_Size='0';
			Est_Own='0';
			Est_Occ='99-9999';
			T_Rate=1;
		Run;
	%End;
%Mend Optional;
%Macro FixSOC(DataIO);
	Data &DataIO (Drop= SOC_Code);
		Set &DataIO;
		Length Occ_Code $7.;
		SubStr(Occ_Code,1,2)=SubStr(SOC_Code,1,2);
		SubStr(Occ_Code,3,1)='-';
		SubStr(Occ_Code,4,4)=SubStr(SOC_Code,3,4);
	Run;
%Mend FixSOC;
%Macro ImportData;
	Data Alternative_SOC;
		infile &Alternative_SOC delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2;
		format SOC_title $250.;
		format SOC_code $6.;
		informat SOC_title $250.;
		informat SOC_code $6.;
		input SOC_title $
		      SOC_code $;
	Run;
	Proc Contents NoPrint Data=Alternative_SOC Out=Contents_Alternative_SOC; Run;
	Data PermData.Alternative_SOC; Set Alternative_SOC; Run;
	Data PartA;
		infile &PartA delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2;
		format SurveyID $6.;
		format Response_status $2.;
		format Date_received mmddyy10.;
		informat SurveyID $6. ;
		informat Response_status $2. ;
		informat Date_received mmddyy10. ;
		input SurveyID $
		      Response_status $
		      Date_received
		      Round_received $
		      Requested_report
		      Respondent $
		      Respondent_title $
		      Respondent_phone $
		      Respondent_phone_extension $
		      Reported_employment
		      No_job_vacancies
		      Respondent_comments $
		      User $;
	Run;
	Proc Contents NoPrint Data=PartA Out=Contents_PartA; Run;
	Data PermData.PartA; Set PartA; Run;
	Data PartB;
		infile &PartB delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2;
		format SurveyID $6.;
		format Part_time_Full_time $1.;
		format Coded_SOC_title $250.;
		format Perm_Seas $1.;
		format Length_open $1.;
		format Education $1.;
		format Cert_Lic $1.;
		format Experience $1.;
		format Wage_Period $1.;
		informat SurveyID $6.;
		informat Part_time_Full_time $1.;
		informat Coded_SOC_title $250.;
		informat Perm_Seas $1.;
		informat Length_open $1.;
		informat Education $1.;
		informat Cert_Lic $1.;
		informat Experience $1.;
		informat Wage_Period $1.;
		input JobID
		      SurveyID $
		      Part_time_Full_time $
		      Reported_job_title $
		      Coded_SOC_title $
		      Perm_Seas $
		      Number_open
		      Length_open $
		      Education $
		      Cert_Lic $
		      Experience $
		      Low_Wage
		      Max_Wage
		      Wage_Period $
		      Benefit1
		      Benefit2
		      Benefit3
		      Benefit4
		      Benefit5
		      No_benefits
		      No_benefit_response
		      User $;
	Run;
	Proc Contents NoPrint Data=PartB Out=Contents_PartB; Run;
	Data PermData.PartB; Set PartB; Run;
	Data Sample;
		infile &Sample delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2;
		format NAICS $6.;
		format SurveyID $6.;
		informat NAICS $6.;
		informat SurveyID $6.;
		input Trans_Code $
		      St_FIPS $
		      Yrqtr $
		      UIAccount $
		      Reptunit $
		      FEIN $
		      PredUIAcct $
		      PredRUN $
		      SuccUIAcct $
		      SuccRUN $
		      Legal_Name $
		      Trade_Name $
		      UI_Addr1 $
		      UI_Addr2 $
		      UI_City $
		      UI_State $
		      UI_Zip $
		      UI_Zip_Ext $
		      PL_Addr1 $
		      PL_Addr2 $
		      PL_City $
		      PL_State $
		      PL_Zip $
		      PL_Zip_Ext $
		      MO_Addr1 $
		      MO_Addr2 $
		      MO_City $
		      MO_State $
		      MO_Zip $
		      MO_Zip_Ext $
		      MO_Addr_Type $
		      Unit_Desc $
		      Phone $
		      Setup_Date $
		      Liab_Date $
		      EOL_Date $
		      React_Date $
		      Status $
		      CES_Ind $
		      Ref_Resp_Code $
		      Refile_Year $
		      Old_County $
		      Old_Ownership $
		      Old_SIC $
		      Old_Town $
		      Old_NAICS $
		      Old_Aux_NAICS $
		      Data_Source $
		      Special_Ind $
		      Agent_Code $
		      SIC $
		      NAICS $
		      Aux_NAICS $
		      Ownership $
		      Org_Type $
		      County $
		      Town $
		      Aux $
		      Mnth1Emp
		      Mnth1Ind $
		      Mnth2Emp
		      Mnth2Ind $
		      Mnth3Emp
		      Mnth3Ind $
		      Total_Wages
		      Total_Wages_Ind $
		      Taxable_Wages
		      Contributions
		      Type_Cov $
		      MEEI $
		      Rept_Change_Ind $
		      Comment1 $
		      Comment2 $
		      Comment3 $
		      Narr_Comment $
		      NAX_Resp_Code $
		      Weight
		      SurveyID $
		      StrataID $;
	Run;
	Proc Contents NoPrint Data=Sample Out=Contents_Sample; Run;
	Data PermData.Sample; Set Sample; Run;
	Data SampleID;
		infile &SampleID delimiter = ',' MISSOVER DSD lrecl=32767 firstobs=2;
		format SurveyID $6.;
		format StateFIPS $2.;
		format County $3.;
		format Township $3.;
		format Industry $6.;
		format Owner $1.;
		format Allocation_Strata $12.;
		informat SurveyID $6.;
		informat StateFIPS $2.;
		informat County $3.;
		informat Township $3.;
		informat Industry $6.;
		informat Owner $1.;
		informat Allocation_Strata $12.;
		input SurveyID $
		      Leg_name $
		      Trade_name $
		      Address $
		      Address2 $
		      City $
		      State $
		      Zip $
		      Phone $
		      StateFIPS $
		      County $
		      Township $
		      Industry $
		      Size $
		      Owner $
		      Spec_area_type $
		      Spec_area $
		      Spec_area_desc $
		      Original_Employment
		      Weight
		      Modified_weight
		      Allocation_Strata $;
	Run;
	Proc Contents NoPrint Data=SampleID Out=Contents_SampleID; Run;
	Data PermData.SampleID; Set SampleID; Run;
	Data Contents_Required (Keep=MemName NObs);
		Set Contents_Alternative_SOC (In=In1)
		Contents_PartA (In=In2)
		Contents_PartB (In=In3)
		Contents_Sample (In=In4)
		Contents_SampleID (In=In5);
		If In1 Then Type=1;
		If In2 Then Type=2;
		If In3 Then Type=3;
		If In4 Then Type=4;
		If In5 Then Type=5;
		If Lag(Type)=Type Then Delete;
	Run;
	/******************************/
	ods listing close;
	ods html body=_webout(dynamic) rs=none style=styles.format2;
	/******************************/
	Title "SAMPLE DATA: Record Count";
	Proc Print NoObs Data=Contents_Required; Run;
	%Optional;
	Data Estab_1;
		Set SampleID;
		SchNum=SubStr(Left(SurveyID),1,5);
		CheckDig=SubStr(Left(SurveyID),6,1);
		FIPS=StateFIPS;
		Town=Township;
		Ori_Emp=Original_Employment;
		OriWgt=Weight;
		CurWgt=Modified_weight;
		AllocStrata=Allocation_Strata;
		If Length(Industry)=2 Then Owner=SubStr(AllocStrata,4,1);
		Else If Length(Industry)=3 Then Owner=SubStr(AllocStrata,5,1);
		Else If Length(Industry)=4 Then Owner=SubStr(AllocStrata,6,1);
		Else If Length(Industry)=5 Then Owner=SubStr(AllocStrata,7,1);
		Else If Length(Industry)=6 Then Owner=SubStr(AllocStrata,8,1);
		Keep SchNum CheckDig FIPS County Town Industry Owner Ori_Emp OriWgt CurWgt AllocStrata;
	Run;
	Data Estab_2;
		Set Sample;
		SchNum=SubStr(Left(SurveyID),1,5);
		CheckDig=SubStr(Left(SurveyID),6,1);
		NAICS=Aux_NAICS;
		Keep SchNum CheckDig NAICS;
	Run;
	Data Estab_3;
		Set PartA;
		SchNum=SubStr(Left(SurveyID),1,5);
		CheckDig=SubStr(Left(SurveyID),6,1);
		Status=Response_status;
		Rpt_Emp=Reported_employment;
		Keep SchNum CheckDig Status Rpt_Emp No_job_vacancies;
	Run;
	Proc Sort Data=Estab_1 NoDupKey; By SchNum CheckDig; Run;
	Proc Sort Data=Estab_2 NoDupKey; By SchNum CheckDig; Run;
	Proc Sort Data=Estab_3 NoDupKey; By SchNum CheckDig; Run;
	Data Estab (Drop= No_job_vacancies);
		Merge Estab_1 (In=In1) Estab_2 (In=In2) Estab_3 (In=In3);
		By SchNum CheckDig;
		If Status='' Then Status='60';
		If CurWgt<=0 Then CurWgt=OriWgt;
		If Rpt_Emp=. Then Rpt_Emp=Ori_Emp;
		If 0<Rpt_Emp<1 Then Rpt_Emp=1;
		Else Rpt_Emp=Round(Rpt_Emp);
		If No_job_vacancies=0 Then Vac_N_Y='Y';
		Else If No_job_vacancies=1 Then Vac_N_Y='N';
		Else Vac_N_Y='X';
	Run;
	Data Detail_1;
		Set SampleID;
		SchNum=SubStr(Left(SurveyID),1,5);
		CheckDig=SubStr(Left(SurveyID),6,1);
		FIPS=StateFIPS;
		Keep SchNum CheckDig FIPS;
	Run;
	Data Detail_2;
		Set Alternative_SOC;
		SOC_Title=SOC_title;
		SOC_Code=SOC_code;
		Keep SOC_Title SOC_Code;
	Run;
	Data Detail_3;
		Set PartB;
		SchNum=SubStr(Left(SurveyID),1,5);
		CheckDig=SubStr(Left(SurveyID),6,1);
		PartFull=Part_time_Full_time;
		SOC_Title=Coded_SOC_title;
		Perm_TS=Perm_Seas;
		Vacancies=Number_open;
		How_Long=Length_open;
		Edu_Lvl=Education;
		Cert_License=Cert_Lic;
		Exp_Lvl=Experience;
		Min_Wage=Low_Wage;
		Benefit6=No_benefits;
		Benefits_RspFlg=No_benefit_response;
		Keep JobID SchNum CheckDig PartFull SOC_Title Perm_TS Vacancies How_Long Edu_Lvl Cert_License Exp_Lvl Min_Wage Max_Wage Wage_Period Benefit1 Benefit2 Benefit3 Benefit4 Benefit5 Benefit6 Benefits_RspFlg;
	Run;
	Data Detail_3;
		Set Detail_3;
		If (PartFull='1') Then PartFull='P';
		Else If (PartFull='2') Then PartFull='F';
		If (Perm_TS='1') Then Perm_TS='P';
		Else If (Perm_TS='2') Then Perm_TS='T';
		Else Perm_TS='X';
		If (Max_Wage=. And Min_Wage^=.) Then Max_Wage=Min_Wage;
		If (Min_Wage=. And Max_Wage^=.) Then Min_Wage=Max_Wage;
		If Benefit1^=0 Then Benefit1=1;
		If Benefit2^=0 Then Benefit2=1;
		If Benefit3^=0 Then Benefit3=1;
		If Benefit4^=0 Then Benefit4=1;
		If Benefit5^=0 Then Benefit5=1;
		If Benefit6^=0 Then Benefit6=1;
		If Benefits_RspFlg^=0 Then Benefits_RspFlg=1;
		If Wage_Period='A' Then Do;
		   Min_Wage=Round(Min_Wage/2080,0.001);
		   Max_Wage=Round(Max_Wage/2080,0.001);
		End;
		If Wage_Period='M' Then Do;
	       Min_Wage=Round(Min_Wage*12/2080,0.001);
		   Max_Wage=Round(Max_Wage*12/2080,0.001);
		End;
	Run;
	Data Detail_4;
		Set Detail_3;
		If (How_long='X' or Perm_TS='X' or Edu_lvl='X' or Exp_lvl='X' or Min_Wage=. or Max_Wage=. or Benefits_RspFlg='1' or Cert_License='X') Then O_Status='P';
		Else O_Status='C';
		Keep JobID O_Status;
	Run;
	Proc Sort Data=Detail_1 NoDupKey; By SchNum CheckDig; Run;
	Proc Sort Data=Detail_2 NoDupKey; By SOC_Title; Run;
	Proc Sort Data=Detail_3; By SchNum CheckDig; Run;
	Proc Sort Data=Detail_4 NoDupKey; By JobID; Run;
	Data Detail_5;
		Merge Detail_1 (In=In1) Detail_3 (In=In3);
		By SchNum CheckDig;
		If In3;
	Run;
	Proc Sort Data=Detail_5; By JobID; Run;
		Data Detail_6 (Drop= Occ_Status);
		Merge Detail_4 (Rename=O_Status=Occ_Status) Detail_5;
		By JobID;
		If Occ_Status='P' Then O_Status='80';
		If ((Occ_Status='C') And (Min_Wage>0) And (Max_Wage>0)) Then O_Status='90';
		Else If (Occ_Status='C' And ((Min_Wage=.) And (Max_Wage=.))) Then O_Status='80';
	Run;
	Proc Sort Data=Detail_6; By SOC_Title; Run;
	Data PermData.Detail;
		Merge Detail_6 (In=In6) Detail_2 (In=In2);
		By SOC_Title;
		If In6;
		If SOC_Code='' Then SOC_Code='000000';
		If Vacancies>0;
	Run;
	%FixSOC(PermData.Detail);
	Proc Sort Data=PermData.Detail Out=Detail_Status; By SchNum CheckDig O_Status; Run;
	Data Detail_Status;
		Set Detail_Status;
		By SchNum CheckDig O_Status;
		If First.CheckDig;
		Keep SchNum CheckDig O_Status;
	Run;
	Proc Sort Data=Estab; By SchNum CheckDig; Run;
	Proc Sort Data=Detail_Status; By SchNum CheckDig; Run;
	Data PermData.Estab;
		Merge Estab (In=InE) Detail_Status (In=InD);
		By SchNum CheckDig;
		If ((O_Status>'') And (O_Status<Status)) Then Status=O_Status;
		Drop O_Status;
	Run;
	/******************************/
	data _null_;
		file _webout;
		put @1 '<br><a href = ' '"' 'http://oessun3/sasweb/IntrNet8/jvsest/runoutlier.html' '"' '><img src = ' '"' 'http://oessun3/sasweb/IntrNet8/images/back.JPG' '"' '></a>';
	run;
	/******************************/
	/******************************/
	ods html close;
	ods listing;
	/******************************/
%Mend ImportData;

/**********************\
End of JVS Importation
\**********************/

/*******************************\
Beginning of JVS Data Screening
\*******************************/

%Macro OutlierTest(DataName,VarName,ESize,LowParm,HighParm);
	Data ObsCount;
		Set &DataName;
		Where (Size=(&ESize./1)) And (&VarName NE .);
	Run;
	Proc Contents Data=ObsCount NoPrint Out=MyObsContents; Run;
	Data _Null_;
		Set MyObsContents;
		If _N_=1;
		Call Symput('DataObs',NObs);
	Run;
	Proc Datasets Lib=Work MemType=Data NoList; Delete MyObsContents ObsCount; Run;
	%If (&DataObs GT 0) %Then %Do;
		Proc Univariate Data=&DataName NoPrint;
			Where (Size=(&ESize./1)) And (&VarName NE .);
			Var &VarName;
			Output Out=QData Q1=Q1 Median=Median Q3=Q3 Min=MinV Max=MaxV;
		Run;
		Data _Null_;
			Set QData;
			d_L=Median-Q1;
			d_U=Q3-Median;
			RangeV=MaxV-MinV;
			Call Symput("RangeV",RangeV);
			Call Symput("Q1",Q1);
			Call Symput("Q2",Median);
			Call Symput("Q3",Q3);
			Call Symput("d_L",d_L);
			Call Symput("d_U",d_U);
		Run;
		Data Outlier0;
			Set &DataName;
			Where ((Size)=(&ESize./1)) And (&VarName NE .);
			Length Data_Name $6. Item_Name $9.;
			Severity='';
			d_l=Max(&d_l,Abs(0.01*&Q2));
			d_u=Max(&d_u,Abs(0.01*&Q2));
			If (&Varname LT (&Q2-(1.0*&LowParm*d_L))) OR (&Varname GT (&Q2+(1.0*&HighParm*d_U))) Then Item_Severity = '* ';
			If (&Varname LT (&Q2-(2.0*&LowParm*d_L))) OR (&VarName GT (&Q2+(2.0*&HighParm*d_U))) Then Item_Severity = '**';
			If (&RangeV=0) Then Item_Severity='';
			If Item_Severity in ('* ','**');
			Lo_Bound=&Q2-(1.0*&LowParm*d_L);
			Hi_Bound=&Q2+(1.0*&HighParm*d_u);
			Data_Name="&DataName";
			Item_Name="&VarName";
			Item_Value=&VarName;
			Range=&RangeV;
			If Occ_Code=" " Then Occ_Code="-------";
			Keep Size SchNum CheckDig Occ_Code Data_Name Item_Name Item_Value Item_Severity Lo_Bound Hi_Bound Range;
		Run;
		Proc Contents Data=Outlier0 NoPrint Out=Contents_Out; Run;
			Data _NULL_;
			Set Contents_Out;
			If _N_=1;
			Call Symput('NumOutliers',NObs);
		Run;
		%If (&NumOutliers GT 0) %Then %Do;
			Proc Append Force Base=Outliers Data=Outlier0;
			Run;
		%End;
	%End;
%Mend OutlierTest;
%Macro DoOutlier(DataName,VarName,LoParm,HiParm);
	%Do EstabSize= 0 %To 9;
		%OutlierTest(&DataName,&VarName,&EstabSize,&LoParm,&HiParm);
	%End;
%Mend DoOutlier;
%Macro ScreenData;
	Data Estab; Set PermData.Estab; Run;
	Data Estab_Error_Recs (Keep= Schnum Var_Name Data_Value);
		Length Data_Value Var_Name $ 12;
		Array Vars (10) $ 12;
		Array Vars_Nam (10) $ 12 ('SchNum' 'CheckDig' 'OriWgt' 'CurWgt' 'Ori_Emp' 'Rpt_Emp' 'County' 'Town' 'NAICS' 'AllocStrata');
		Set Estab;
		Vars(1)=SchNum;
		Vars(2)=CheckDig;
		Vars(3)=put(OriWgt,9.3);
		Vars(4)=put(CurWgt,9.3);
		Vars(5)=put(Ori_Emp,8.);
		Vars(6)=put(Rpt_Emp,8.);
		Vars(7)=County;
		Vars(8)=Town;
		Vars(9)=NAICS;
		Vars(10)=AllocStrata;
		Do i=1 To 10;
			Var_Name=Vars_Nam(i);
			Data_Value=Vars(i);
			If Data_Value='' Then Output;
			Else Do;
				If i in (1 2 3 4 5 6 7 8 9) Then Do;
					_error_=0;
					Var_Num=input(Data_Value, ? 12.);
					If _error_=1 Then Output;
				End;
				If i in (3 4 5 6) Then Do;
					pos=index(Var_Num,'E');
					If pos^=0 Then Output;
				End;
				If i in (1 3 4 9) And (Var_Num<1) Then Output;
				Else If (Var_Num<0) then Output;
			End;
		End;
		Var_Name='Owner';
		Data_Value=Owner;
		If Data_Value Not In ('0' '1' '2' '3' '5' '6' '7') Then Output;
		Var_Name='Vac_N_Y';
		Data_Value= Vac_N_Y;
		If Data_Value Not In ('N' 'Y' 'X') Then Output;
		Var_Name='FIPS';
		Data_Value=FIPS;
		If Data_Value Not in ('01' '02' '04' '05' '06' '08' '09' '10' '11' '12' '13' '15' '16' '17' '18' '19' '20' '21' '22' '23'
			                  '24' '25' '26' '27' '28' '29' '30' '31' '32' '33' '34' '35' '36' '37' '38' '39' '40' '41' '42' '44'
			                  '45' '46' '47' '48' '49' '50' '51' '53' '54' '55' '56' '60' '66' '72' '78' '99') Then Output;
		Var_Name='Status';
		Data_Value=Status;
		If Data_Value Not in ('10' '20' '30' '31' '33' '40' '60' '70' '80' '81' '82' '83' '84' '85' '86' '90' '91' '92' '93' '94' '95' '96') Then Output;
	Run;
	Proc Sort Data=Estab_Error_Recs NoDupKey; By SchNum Var_Name; Run;
	Data Detail; Set PermData.Detail; Run;
	Data Detail_Error_Recs (Keep= SchNum Occ_Code Var_Name Data_Value);
		Length Var_Name Data_Value Data_Value_i $ 12;
		Array Vars(5) $ 12;
		Array Vars_Ben(6) $ 1;
		Array Vars_Nam(5) $ 12 ('SchNum' 'CheckDig' 'Est_Occ' 'Vacancies' 'Occ_Code');
		Set Detail;
		Vars(1)=SchNum;
		Vars(2)=CheckDig;
		Vars(3)='00-0000';
		Vars(4)=put(Vacancies,6.);
		Vars(5)=Occ_Code;
		Vars_Ben(1)=Benefit1;
		Vars_Ben(2)=Benefit2;
		Vars_Ben(3)=Benefit3;
		Vars_Ben(4)=Benefit4;
		Vars_Ben(5)=Benefit5;
		Vars_Ben(6)=Benefit6;
		Do i=1 To 5;
			Var_Name=Vars_Nam(i);
			Data_Value=Vars(i);
			If (Data_Value='') Then Output;
			Else Do;
				_error_=0;
				If i in (3 5) Then Do;
					Data_Value_i=compress(Vars(i),'-');
					Var_Num=Input(Data_Value_i, ? 12.);
				End;
				Else Var_Num=Input(Data_Value, ? 12.);
				If _error_ = 1 Then Output;
				If i in (4 6 7) Then Do;
					pos=index(Var_Num,'E');
					If pos^=0 Then Output;
				End;
				If i in (6 7) And Not (1.00<=Var_Num<=500.00) Then Output;
				Else If i in (1) And Var_Num<1 Then Output;
				Else If Var_Num<0 Then Output;
			End;
		End;
		Var_Name='PartFull';
		Data_Value=PartFull;
		If Data_Value Not in ('P' 'F') Then Output;
		Var_Name='Perm_TS';
		Data_Value=Perm_TS;
		If Data_Value Not in ('P' 'T' 'X') Then Output;
		Var_Name='How_Long';
		Data_Value=How_Long;
		If Data_Value Not in ('1' '2' '3' '4' 'X') Then Output;
		Var_Name='Edu_Lvl';
		Data_Value=Edu_Lvl;
		If Data_Value Not in ('N' 'H' 'V' '2' 'B' 'A' 'X') Then Output;
		Var_Name='Exp_Lvl';
		Data_Value=Exp_Lvl;
		If Data_Value Not in ('N' 'W' 'R' 'X') Then Output;
		Do i=1 To 6;
			Var_Name='Benefit'||put(i,1.);
			Data_Value=Vars_Ben(i);
			If Data_Value Not in ('0' '1') Then Output;
		End;
		Var_Name='Cert_License';
		Data_Value=Cert_License;
		If Data_Value Not in ('0' '1' 'X') Then Output;
		Var_Name='FIPS';
		Data_Value=FIPS;
		If Data_Value Not in ('01' '02' '04' '05' '06' '08' '09' '10' '11' '12' '13' '15' '16' '17' '18' '19' '20' '21' '22' '23'
                              '24' '25' '26' '27' '28' '29' '30' '31' '32' '33' '34' '35' '36' '37' '38' '39' '40' '41' '42' '44'
                              '45' '46' '47' '48' '49' '50' '51' '53' '54' '55' '56' '60' '66' '72' '78' '99') Then Output;
	Run;
	Proc Sort Data=Detail_Error_Recs NoDupKey; By SchNum Occ_Code Var_Name; Run;
	/******************************/
	ods listing close;
	ods html body=_webout(dynamic) rs=none style=styles.format2;
	/******************************/
	Title "BASIC EDITS: Estab Errors";
	Proc Print Data=Estab_Error_Recs; Run;
	Title "BASIC EDITS: Detail Errors";
	Proc Print Data=Detail_Error_Recs; Run;
	Data Estab SizeData (Keep= SchNum CheckDig Size);
		Set PermData.Estab;
		Emp_Diff=abs(Ori_Emp-Rpt_Emp);
		Wgt_Diff=abs(OriWgt-CurWgt);
		If Status GE '80';
		Size = SubStr(AllocStrata,1,1)/1;
		Output Estab;
		Output SizeData;
	Run;
	Proc Sort Data=SizeData NoDupKey; By SchNum CheckDig; Run;
	Proc Sort Data=PermData.Detail Out=Detail; By SchNum CheckDig Occ_Code; Run;
	Data Detail;
		Merge SizeData (In=InSize) Detail (In=InDetail);
		By SchNum CheckDig;
		If InDetail;
		If O_Status GE '80';
	Run;
	%DoOutlier(Estab,Emp_Diff,&dEmp_Lo.,&dEmp_Hi.);
	%DoOutlier(Estab,Wgt_Diff,&dWgt_Lo.,&dWgt_Hi.);
	%DoOutlier(Detail,Vacancies,&Vacnc_Lo.,&Vacnc_Hi.);
	%DoOutlier(Detail,Min_Wage,&LWage_Lo.,&LWage_Hi.);
	%DoOutlier(Detail,Max_Wage,&UWage_Lo.,&UWage_Hi.);
	Proc Contents Data=Outliers NoPrint Out=Contents_Out; Run;
	Data _NULL_;
		Set Contents_Out;
		If _N_=1;
		Call Symput('NumRecords',NObs);
	Run;
	Proc Sort Data=Outliers NoDupKey Out=Outlier1; By SchNum CheckDig; Run;
	Proc Contents Data=Outlier1 NoPrint Out=Contents_Out; Run;
	Data _NULL_;
		Set Contents_Out;
		If _N_=1;
		Call Symput('NumScheds',NObs);
	Run;
	Proc Freq Data=Outliers Noprint; Tables Item_Name /Out=MyFreq NoRow NoCol noPercent; Run;
	Title "OUTLIERS: Counts by Variable";
	Title3 "Outlier Schedules: &NumScheds.";
	Title4 "Outlier Records : &NumRecords.";
	Proc Print NoObs Data=MyFreq; Var Item_Name Count; Run;
	Proc Sort Data=Outliers; By SchNum CheckDig Descending Item_Severity; Run;
	Data Outliers_Scheds;
		Set Outliers;
		By SchNum CheckDig Descending Item_Severity;
		If First.CheckDig;
		Schedule_Severity=Item_Severity;
		Keep SchNum CheckDig Schedule_Severity;
	Run;
	Proc Freq Data=Outliers NoPrint; Tables SchNum * CheckDig /out=Item_Counts (Keep=SchNum CheckDig Count); Run;
	Proc Sort Data=Outliers; By SchNum CheckDig; Run;
	Proc Sort Data=Outliers_Scheds; By SchNum CheckDig; Run;
	Data Outliers;
		Merge Outliers_Scheds Item_Counts (rename=(Count=Item_Count)) Outliers;
		By SchNum CheckDig;
		Label Item_Count="Item Count";
		Label Schedule_Severity="Schedule Severity";
	Run;
	Proc Sort Data=Outliers; By Data_Name Descending Schedule_Severity Descending Size Descending Item_Count Descending Item_Severity; Run;
	Title "OUTLIERS: Listed by Schedule";
	Title3 "Outlier Schedules: &NumScheds.";
	Title4 "Outlier Records : &NumRecords.";
	Proc Print Data=Outliers Split="_";
		Var SchNum CheckDig Schedule_Severity Size Item_Count Data_Name Occ_Code Item_Name Item_Value Item_Severity Lo_Bound Hi_Bound Range;
	Run;
	/******************************/
	data _null_;
		file _webout;
		put @1 '<br><a href = ' '"' 'http://oessun3/jvsest/index.html' '"' '><img src = ' '"' 'http://oessun3/sasweb/IntrNet8/images/back.JPG' '"' '></a>';
	run;
	/******************************/
	/******************************/
	ods html close;
	ods listing;
	/******************************/
%Mend ScreenData;

/*************************\
End of JVS Data Screening
\*************************/

/********************************\
Beginning of JVS Unit Imputation
\********************************/

%Macro EstabImpute;
	%Global max_i;
	Data TmpEstab;
		Set PermData.Estab;
		If (20<=Status<=29) Or (60<=Status<=99);
		Size=SubStr(AllocStrata,1,1);
		EstCell=SubStr(AllocStrata,2,7);
	Run;
	Data Viabl_Usbl (Keep= EstCell Size Usable_n Usable_Emp Viable_n Viable_Emp);
		Set TmpEstab (Keep= EstCell Size Status CurWgt Ori_Emp);
		Length Usable_n Usable_Emp Viable_n Viable_Emp 8;
		Usable_n=0;
		Usable_Emp=0;
		Viable_n=0;
		Viable_Emp=0;
		Wgt_Emp=CurWgt*Ori_Emp;
		If (80<=Status<=99) Then Do;
			Usable_n=1;
			Usable_Emp=Wgt_Emp;
		End;
		If (20<=Status<=29) Or (60<=Status<=99) Then Do;
			Viable_n=1;
			Viable_Emp=Wgt_Emp;
		End;
	Run;
	Proc Sort Data=Viabl_Usbl; By EstCell Size; Run;
	Proc Means Data=Viabl_Usbl NoPrint;
		By EstCell Size;
		Var Viable_n Usable_n Viable_Emp Usable_Emp;
		Output Out=Summed_to_Size Sum(Viable_n Usable_n Viable_Emp Usable_Emp)=V_n U_n V_Emp U_Emp;
	Run;
	Data Usable_key (Keep= EstCell Size);
		Set TmpEstab (Keep= EstCell Status Size);
		If Not (Status<80);
	Run;
	Proc Sort Data=Usable_key NoDupKey; By EstCell Size; Run;
	Data By_Size;
	Attrib Flag Format=3. NRAF Format=6.2 MaxF Format=4.2;
		Set Summed_to_Size;
		If U_Emp>0 Then NRAF=V_Emp/U_Emp;
		Else If V_Emp=0 Then NRAF=-1.00;
		Else NRAF=-99.999;
		If 0=<U_n<=4 Then MaxF=3.00;
		Else If U_n=5 Then MaxF=3.20;
		Else If U_n=6 Then MaxF=3.34;
		Else If U_n=7 Then MaxF=3.58;
		Else If U_n=8 Then MaxF=3.75;
		Else If U_n>8 Then MaxF=4.00;
		Flag=0;
		If NRAF>MaxF Then Flag=1;
		Else If NRAF=-99.999 Then Flag=1;
	Run;
	Proc Sort Data=By_Size; By EstCell; Run;
	Proc Means Data=By_Size NoPrint;
		By EstCell;
		Var V_n U_n V_Emp U_Emp Flag;
		Output Out=Summed_to_EstCell Sum(V_n U_n V_Emp U_Emp Flag)=V_n0 U_n0 V_Emp0 U_Emp0 Flag0;
	Run;
	Data By_EstCell;
		Set Summed_to_EstCell;
		If U_Emp0>0 Then NRAF0=V_Emp0/U_Emp0;
		Else If V_Emp0=0 Then NRAF0=-1.00;
		Else NRAF0=-99.999;
	Run;
	Data Summed_Data;
		Merge By_Size By_EstCell;
		By EstCell;
		If Flag0>0 Then NRAF=NRAF0;
	Run;
	Proc Sort Data=TmpEstab Out=TmpEstabSz NoDupKey; By Descending Size; Run;
	Data _NULL_;
		Set TmpEstabSz (Obs=1);
		Call Symput('max_i',Size);
	Run;
	Data INRAF (Drop= InSize InV_n InU_n InV_Emp InU_Emp InNRAF InMaxF InFlag Collapsed i Titl Size0-Size&max_i) REPORT_recs (Keep= EstCell Collapsed Titl Size0-Size&max_i);
		Attrib NRAF1-NRAF&max_i Format=6.2 MaxF1-MaxF&max_i Format=4.2 Titl Length=$7;
		Retain U_n1-U_n&max_i U_Emp1-U_Emp&max_i V_n1-V_n&max_i V_Emp1-V_Emp&max_i NRAF1-NRAF&max_i MaxF1-MaxF&max_i Flag1-Flag&max_i;
		Array V_n(&max_i) V_n1-V_n&max_i;
		Array U_n(&max_i) U_n1-U_n&max_i;
		Array V_Emp(&max_i) V_Emp1-V_Emp&max_i;
		Array U_Emp(&max_i) U_Emp1-U_Emp&max_i;
		Array NRAF(&max_i) NRAF1-NRAF&max_i;
		Array MaxF(&max_i) MaxF1-MaxF&max_i;
		Array Flag(&max_i) Flag1-Flag&max_i;
		Array Size(&max_i) Size1-Size&max_i;
		Set Summed_Data (Rename= (V_n=InV_n U_n=InU_n V_Emp=InV_Emp U_Emp=InU_Emp NRAF=InNRAF MaxF=InMaxF Flag=InFlag Size=InSize));
		By EstCell InSize;
		If first.EstCell Then Do i=1 to &max_i;
			V_n(i)=0;
			U_n(i)=0;
			V_Emp(i)=0;
			U_Emp(i)=0;
			NRAF(i)=0;
			MaxF(i)=0;
			Flag(i)=0;
		End;
		i=InSize;
		V_n(i)=InV_n;
		U_n(i)=InU_n;
		V_Emp(i)=InV_Emp;
		U_Emp(i)=InU_Emp;
		NRAF(i)=InNRAF;
		MaxF(i)=InMaxF;
		Flag(i)=InFlag;
		If last.EstCell Then Do;
			Output INRAF;
			If Flag0>=1 Then Collapsed='Yes';
			Else Collapsed='No';
			Titl='V_n_i';
			Size0=V_n0;
			Do i=1 To &max_i;
				Size(i)=V_n(i);
			End;
			Output REPORT_recs;
			Titl='U_n_i';
			Size0=U_n0;
			Do i=1 To &max_i;
				Size(i)=U_n(i);
			End;
			Output REPORT_recs;
			Titl='V_Emp_i';
			Size0=V_Emp0;
			Do i=1 To &max_i;
				Size(i)=V_Emp(i);
			End;
			Output REPORT_recs;
			Titl='U_Emp_i';
			Size0=U_Emp0;
			Do i=1 To &max_i;
				Size(i)=U_Emp(i);
			End;
			Output REPORT_recs;
			Titl='NRAF_i';
			Size0=NRAF0;
			Do i=1 To &max_i;
				Size(i)=NRAF(i);
			End;
			Output REPORT_recs;
			Titl='Flag_i';
			Size0=Flag0;
			Do i=1 To &max_i;
				Size(i)=Flag(i);
			End;
			Output REPORT_recs;
		End;
	Run;
	Data FNRAF (Keep= EstCell Size NRAF);
		Merge Summed_Data (In=in_SD) Usable_key (In=in_UK);
		By EstCell Size;
		If in_SD;
	Run;
	Proc SQL;
		CREATE Table FNRAF AS
		SELECT EstCell, Size, NRAF
		FROM FNRAF;
	Quit;
	Proc Sort Data=FNRAF; By EstCell Size; Run;
	Proc Sort Data=TmpEstab; By EstCell Size; Run;
	Data PermData.F_Estab;
		Merge FNRAF (In=in_FN) TmpEstab (In=in_TE);
		By EstCell Size;
		If in_TE;
		If Status=10 Then Delete;
		If Status=20 Then Delete;
		If Status=30 Then Delete;
		If Status=31 Then Delete;
		If Status=33 Then Delete;
		If Status=40 Then Delete;
		If Status=60 Then Delete;
		If Status=70 Then Delete;
	Run;
	Data NRAF_Report;
		Set PermData.F_Estab (Keep=AllocStrata NRAF);
	Run;
	Proc Sort Data=NRAF_Report NoDupKey; By AllocStrata; Run;
	/******************************/
	ods listing close;
	ods html body=_webout(dynamic) rs=none style=styles.format2;
	/******************************/
	Title "UNIT IMPUTATION: Weighting Class Adjustment (NRAF)";
	Proc Print Data=NRAF_Report; Var AllocStrata NRAF; Run;
	Proc Sort Data=PermData.F_Estab; By EstCell Size; Run;
	/******************************/
	data _null_;
		file _webout;
		put @1 '<br><a href = ' '"' 'http://oessun3/jvsest/index.html' '"' '><img src = ' '"' 'http://oessun3/sasweb/IntrNet8/images/back.JPG' '"' '></a>';
	run;
	/******************************/
	/******************************/
	ods html close;
	ods listing;
	/******************************/
%Mend EstabImpute;

/**************************\
End of JVS Unit Imputation
\**************************/

/********************************\
Beginning of JVS Item Imputation
\********************************/

%Macro Impute(impcell,sortgrp,ImpLvl);
	%If &RecipCnt=0 or &DonorCnt=0 %Then %Do;
		EndSAS;
	%End;
	Data Recips;
		Set Recips;
		r_row=_n_;
		Imputation_Status='no donor';
	Run;
	Data Donors;
		Set Donors;
		d_row=_n_;
		Imputation_Status='available';
	Run;
	Proc Sort Data=Donors; By &impcell; Run;
	Proc Means NoPrint;
		By &impcell;
		Var Vacancies;
		Output Out=donor_cell_count n(Vacancies)=donor_cell_count;
	Run;
	Proc Sort Data=donor_cell_count; By Descending donor_cell_count; Run;
	Data _NULL_;
		Set donor_cell_count (Keep= donor_cell_count);
		If _n_=1;
		If _n_=1 Then Call SymPut('donor_cell_count',donor_cell_count);
	Run;
	Proc Sort Data=Recips; By &impcell Descending Vacancies; Run;
	Data R (Keep= &impcell MaxVcy Rpt_Emp);
		Retain MaxVcy;
		Set Recips;
		By &impcell;
		If first.&sortgrp Then MaxVcy=Vacancies;
	Run;
	Proc Sort Data=R; By &impcell Descending Rpt_Emp; Run;
	Data Rmax (Keep= &impcell MaxVcy MaxREmp);
		Retain MaxREmp;
		Set R;
		By &impcell;
		If first.&sortgrp Then Do;
			MaxREmp=Rpt_Emp;
			Output;
		End;
	Run;
	Proc Sort Data=Rmax; By &impcell; Run;
	Proc Sort Data=Donors; By &impcell Descending Vacancies; Run;
	Data D (Keep= &impcell MaxVcy Rpt_Emp);
		Retain MaxVcy;
		Set Donors;
		By &impcell;
		If first.&sortgrp Then;
			MaxVcy=Vacancies;
	Run;
	Proc Sort Data=D; By &impcell Descending Rpt_Emp; Run;
	Data DMax (Keep= &impcell MaxVcy MaxREmp);
		Retain MaxREmp;
		Set D;
		By &impcell;
		If first.&sortgrp Then Do;
			MaxREmp=Rpt_Emp;
			Output;
		End;
	Run;
	Proc Sort Data=DMax; By &impcell; Run;
	Data Max;
		Set Rmax (Rename=(MaxREmp=Rpt_Emp MaxVcy=Vacancies)) Dmax (Rename=(MaxREmp=Rpt_Emp MaxVcy=Vacancies));
		By &impcell;
	Run;
	Proc Sort Data=Max; By &impcell Descending Vacancies; Run;
	Data Max (Keep= &impcell MaxVcy Rpt_Emp);
		Set Max;
		By &impcell;
		Retain MaxVcy;
		If first.&sortgrp Then;
			MaxVcy=Vacancies;
	Run;
	Proc Sort Data=Max; By &impcell Descending Rpt_Emp; Run;
	Data Max(Keep= &impcell MaxVcy MaxREmp);
		Retain MaxREmp;
		Set Max;
		By &impcell;
		If first.&sortgrp Then Do;
			MaxREmp=Rpt_Emp;
			Output;
		End;
	Run;
	Proc Sort Data=Max; By &impcell; Run;
	Data Recips;
		Merge Recips (In=R) Max;
		By &impcell;
		If R;
	Run;
	Proc Sort Data=Recips; By &impcell Descending Vacancies; Run;
	Data Index (Keep= &impcell first last);
		Retain first;
		Set Recips;
		By &impcell;
		If first.&sortgrp Then first=_N_;
		If last.&sortgrp Then Do;
			last=_N_;
			Output;
		End;
	Run;
	%Let donors_flg=0;
	Data Donors_I;
		Merge Index (In=I) Donors (In=D Rename=(Vacancies=donrVcys Rpt_Emp=donrREmp
		SchNum=donrSchd OC_3=donrOC_3));
		By &impcell;
		If I and D;
		If I And D Then Call SymPut('donors_flg',1);
	Run;
	%If &donors_flg=0 %Then %GoTo endimput;
	Data Rcp_Dnr&implvl(Keep= &impcell Vacancies donrVcys Rpt_Emp donrREmp SchNum donrSchd OC_3 donrOC_3 MaxVcy MaxREmp r_row d_row Occ_Code JobId);
		Set Donors_I(rename=(Occ_Code=donrOC JobId=donrJId));
		Do recno=first to last;
			Set Recips point=recno;
			Output;
		End;
	Run;
	%If &I=0 %Then %GetDonr1;
	%Else %GetDonr2;
	Proc Sort Data=Recips; By r_row; Run;
	Proc Sort Data=R_marker; By r_row; Run;
	Data Recips For_Imp;
		Merge Recips(In=in1) R_marker(In=in2);
		By r_row;
		If in1;
		If in2 Then Output For_Imp;
		Else If (in1 And Not in2) Then Output Recips;
	Run;
	Proc Sort Data=Donors; By d_row; Run;
	Proc Sort Data=D_marker; By d_row; Run;
	Data Donors Donr_Set;
	Merge Donors(In=in1) D_marker(In=in2);
		By d_row;
		If in1;
		If in2 Then Do;
			Imputation_Status='unavailable';
			Output Donr_Set;
		End;
		Else If (in1 And Not in2) Then Output Donors;
	Run;
	Data _NULL_;
		Set For_Imp end=last;
		If last Then Call SymPut('ImputCnt',_N_);
	Run;
	Data _NULL_;
		Set Donr_Set end=last;
		If last Then Call SymPut('UsedCnt',_N_);
	Run;
	%If %Eval(&RecipCnt-&ImputCnt)^=0 %Then %Do;
		Data _NULL_;
			Set Recips end=last;
			If last Then Call SymPut('RecipCnt',_N_);
		Run;
	%End;
	%Else %Let RecipCnt=0;
	%If &I=1 %Then %Let DonorCnt=&AllDonrCnt;
	%Else %If %Eval(&DonorCnt-&UsedCnt)^=0 %Then %Do;
		Data _NULL_;
			Set Donors end=last;
			If last Then Call SymPut('DonorCnt',_N_);
		Run;
	%End;
	%Else %Let DonorCnt=0; Run;
	Data Donr_imp;
		Set Donr_Set (Rename=(SchNum=donrSchd Occ_Code=donrOcc How_Long=DonrHL
		Edu_Lvl=DonrEdL Exp_Lvl=DonrExL Min_Wage=DonrWMin
		Max_Wage=DonrWMax Perm_TS=DonrPTS Benefit1=DonrBenefit1
		Benefit2=DonrBenefit2 Benefit3=DonrBenefit3
		Benefit4=DonrBenefit4
		Benefit5=DonrBenefit5 Benefit6=DonrBenefit6
		Cert_License=DonrCL));
	Run;
	Proc Sort Data=For_Imp; By r_row; Run;
	Proc Sort Data=Donr_imp; By r_row; Run;
	Data Impute (Drop= DonrHL DonrEdL DonrExL DonrWMin DonrWMax DonrPTS DonrBenefit1 DonrBenefit2 DonrBenefit3 DonrBenefit4 DonrBenefit5 DonrBenefit6 DonrCL);
		Merge For_Imp Donr_imp (Keep= r_row DonrHL DonrEdL DonrExL DonrWMin DonrWMax DonrPTS DonrBenefit1 DonrBenefit2 DonrBenefit3 DonrBenefit4 DonrBenefit5 DonrBenefit6 DonrCL DonrSchd DonrOcc);
		By r_row;
		ImpLvl="&ImpLvl";
		Imputation_Status='donor found';
		If How_Long='X' Then Do;
			How_Long=DonrHL;
			Iflg_HL=1;
		End;
		If Perm_TS='X' Then Do;
			Perm_TS=DonrPTS;
			Iflg_PTS=1;
		End;
		If Edu_Lvl='X' Then Do;
			Edu_Lvl=DonrEdL;
			Iflg_EdL=1;
		End;
		If Exp_Lvl='X' Then Do;
			Exp_Lvl=DonrExL;
			Iflg_ExL=1;
		End;
		If ((Min_Wage='.') Or (Max_Wage='.')) Then Do;
			Min_Wage=DonrWMin;
			Max_Wage=DonrWMax;
			Iflg_W=1;
		End;
		If Benefits_RspFlg='1' Then Do;
			Benefit1=DonrBenefit1;
			Benefit2=DonrBenefit2;
			Benefit3=DonrBenefit3;
			Benefit4=DonrBenefit4;
			Benefit5=DonrBenefit5;
			Benefit6=DonrBenefit6;
			Iflg_Bft=1;
		End;
		If Cert_License='X' Then Do;
			Cert_License=DonrCL;
			Iflg_CL=1;
		End;
	Run;
	Proc Append Base=Used Data=Donr_Set; Run;
	Proc Append Base=Imputed Data=Impute; Run;
	%endimput:;
%Mend Impute;
%Macro GetDonr1;
	Proc Sort Data=Rcp_Dnr&implvl; By &impcell SchNum JobID; Run;
	%Global donor_cell_count;
	Data R_marker(Keep= r_row Distance) D_marker(Keep= d_row r_row);
		Array sn_used(&donor_cell_count) $5;
			Length hold_sn $5;
			Retain j &donor_cell_count usedcntr hold_d_row min_dist 0 hold_sn sn_used;
			Set Rcp_Dnr&implvl;
			By &impcell SchNum JobID;
			A=Abs(DonrVcys-Vacancies)/MaxVcy;
			B=Abs(DonrREmp-Rpt_Emp)/MaxREmp;
			C=Abs(donrOC_3-OC_3)/999999;
			Distance=1/2*A+1/4*B+1/4*C;
			If first.&sortgrp Then Do;
				Do i=1 To j;
				sn_used(i)=' ';
			End;
			usedcntr=0;
		End;
		If first.JobID Then min_dist=1000000;
		If abs(distance)<min_dist Then ok=1;
		Else ok=0;
		If ok=1 Then Do I=1 To j;
			If donrschd=sn_used(I) Then ok=0;
		End;
		If ok=1 Then Do;
			min_dist=abs(distance);
			hold_d_row=d_row;
			hold_sn=donrschd;
		End;
		If last.JobID And (min_dist^=1000000) Then Do;
			usedcntr+1;
			d_row=hold_d_row;
			sn_used(usedcntr)=hold_sn;
			distance=min_dist;
			Output D_marker;
			Output R_marker;
		End;
	Run;
%Mend GetDonr1;
%Macro GetDonr2;
	Data Rcp_Dnr&implvl;
		Set Rcp_Dnr&implvl;
		A=Abs(DonrVcys-Vacancies)/MaxVcy;
		B=Abs(DonrREmp-Rpt_Emp)/MaxREmp;
		C=Abs(donrOC_3-OC_3)/999999;
		Distance=1/2*A+1/4*B+1/4*C;
	Run;
	Proc Sort Data=Rcp_Dnr&implvl; By &impcell SchNum Occ_Code JobID Distance; Run;
	Data R_marker(keep=r_row Distance) D_marker(keep=d_row r_row);
		Set Rcp_Dnr&implvl;
		By &impcell SchNum Occ_Code JobID Distance;
		If first.JobID Then Do;
			Output R_marker;
			Output D_marker;
		End;
	Run;
%Mend GetDonr2;
%Macro all_donors;
	Proc Sort Data=Donors; By SchNum Occ_Code; Run;
	Proc Sort Data=Used; By SchNum Occ_Code; Run;
	Data Donors;
		Set Donors Used;
		By SchNum Occ_Code;
	Run;
	Proc Sort Data=Donors NoDupKey; By SchNum Occ_Code JobID; Run;
	Data _NULL_;
		Set Donors end=last;
		If last Then Call SymPut('AllDonrCnt',_N_);
	Run;
%Mend all_donors;
%Macro no_more_donors;
	Data _NULL_;
		Put 'The donors were used up before imputation of all recipients.' / 'Program has terminated.';
		EndSAS;
	Run;
%Mend no_more_donors;

%Macro Groups;
	%Let I=0;
		%Impute(Occ_Code PartFull N5 Owner Size Area, Area, 1);
	%If &RecipCnt=0 %Then %GoTo EndGroups;
	%If &DonorCnt=0 %Then %no_more_donors;
		%Impute(Occ_Code PartFull N4 Owner Size Area, Area, 2);
	%If &RecipCnt=0 %Then %GoTo EndGroups;
	%If &DonorCnt=0 %Then %no_more_donors;
		%Impute(Occ_Code PartFull N3 Owner Size Area, Area, 3);
	%If &RecipCnt=0 %Then %GoTo EndGroups;
	%If &DonorCnt=0 %Then %no_more_donors;
		%Impute(Occ_Code PartFull N2 Owner Size Area, Area, 4);
	%If &RecipCnt=0 %Then %GoTo EndGroups;
	%If &DonorCnt=0 %Then %no_more_donors;
		%Impute(Occ_Code PartFull N5 Owner Size, Size, 5);
	%If &RecipCnt=0 %Then %GoTo EndGroups;
	%If &DonorCnt=0 %Then %no_more_donors;
		%Impute(Occ_Code PartFull N4 Owner Size, Size, 6);
	%If &RecipCnt=0 %Then %GoTo EndGroups;
	%If &DonorCnt=0 %Then %no_more_donors;
		%Impute(Occ_Code PartFull N3 Owner Size, Size, 7);
	%If &RecipCnt=0 %Then %GoTo EndGroups;
	%If &DonorCnt=0 %Then %no_more_donors;
		%Impute(Occ_Code PartFull N2 Owner Size, Size, 8);
	%If &RecipCnt=0 %Then %GoTo EndGroups;
	%If &DonorCnt=0 %Then %no_more_donors;
		%Impute(Occ_Code PartFull N5, N5, 9);
	%If &RecipCnt=0 %Then %GoTo EndGroups;
	%If &DonorCnt=0 %Then %no_more_donors;
		%Impute(Occ_Code PartFull N4, N4, 10);
	%If &RecipCnt=0 %Then %GoTo EndGroups;
	%If &DonorCnt=0 %Then %no_more_donors;
		%Impute(Occ_Code PartFull N3, N3, 11);
	%If &RecipCnt=0 %Then %GoTo EndGroups;
	%If &DonorCnt=0 %Then %no_more_donors;
		%Impute(Occ_Code PartFull N2, N2, 12);
	%If &RecipCnt=0 %Then %GoTo EndGroups;
	%If &DonorCnt=0 %Then %no_more_donors;
		%Impute(Occ_Code PartFull N1, N1, 13);
	%If &RecipCnt=0 %Then %GoTo EndGroups;
	%If &DonorCnt=0 %Then %no_more_donors;
	%Let I=1;
		%all_donors;
		%Impute(OC_2 N1, N1, 14);
	%If &RecipCnt=0 %Then %GoTo EndGroups;
	%If &DonorCnt=0 %Then %no_more_donors;
		%all_donors;
		%Impute(OC_2, OC_2, 15);
	%If &RecipCnt=0 %Then %GoTo EndGroups;
	%If &DonorCnt=0 %Then %no_more_donors;
	Run;
	Title "ITEM IMPUTATION DID NOT COMPLETE SUCCESSFULLY";
	Title3 "THESE RECORDS WERE NOT IMPUTED:";
	Proc Print Data=Recips; Var SchNum CheckDig Occ_Code; Run;
	Title;
	Title3;
	%EndGroups:;
%Mend Groups;
%Macro ItemImpute;
	%Global ImpLvl DtlCnt1ErrCnt RecipCnt DonorCnt ImputCnt UsedCnt AllDonrCnt I DtlCnt2 donor_cell_count donors_flg;
	Data Estab (Keep= SchNum CheckDig Owner Size County Town NAICS Rpt_Emp);
		Set PermData.F_Estab (Keep= SchNum CheckDig Owner County Town Status AllocStrata NAICS Rpt_Emp);
		If Status>='80';
		Size=SubStr(Left(AllocStrata),1,1);
	Run;
	Proc Sort Data=Estab NoDupKey; By SchNum CheckDig; Run;
	Proc Sort Data=PermData.Detail; By SchNum CheckDig; Run;
	Data Detail;
		Length ImpLvl Iflg_HL Iflg_EdL Iflg_ExL Iflg_W Iflg_PTS Iflg_Bft Iflg_CL 3.;
		Retain ImpLvl Iflg_HL Iflg_EdL Iflg_ExL Iflg_W Iflg_PTS Iflg_Bft Iflg_CL 0;
		Merge Estab (In=In_E) PermData.Detail (In=In_D) End=last;
		By SchNum CheckDig;
		N5=Substr(Left(NAICS),1,5);
		N4=SubStr(Left(NAICS),1,4);
		N3=SubStr(Left(NAICS),1,3);
		N2=SubStr(Left(NAICS),1,2);
		N1=SubStr(Left(NAICS),1,1);
		OC_2=SubStr(Left(Occ_Code),1,2);
		OC_3=SubStr(Left(Occ_Code),1,2)||SubStr(Left(Occ_Code),4,4);
		Area=County||Town;
		If In_E And In_D;
	Run;
	Data _NULL_ ;
		Set Detail End=last;
		If last Then Call SymPut('DtlCnt1',_n_);
	Run;
	Data Donors (Drop= i j k) Recips (Drop= i j k) Error_Recs(Drop= i j k);
		Retain i j k 0;
		Set Detail End=last;
		If O_Status>='90' Then Do;
			i+1;
			Output Donors;
		End;
		Else If '80'<=O_Status<='89' Then Do;
			j+1;
			Output Recips;
		End;
		Else If O_Status<'80' Then Do;
			k+1;
			Output Error_Recs;
		End;
		If last Then Do;
			Call SymPut('DonorCnt',i);
			Call SymPut('RecipCnt',j);
			Call SymPut('ErrCnt',k);
		End;
	Run;
	/******************************/
	ods listing close;
	ods html body=_webout(dynamic) rs=none style=styles.format2;
	/******************************/
	%Groups;
	%all_donors;
	Data PermData.F_Detail (Drop=Rpt_Emp N1 N2 N3 N4 N5 OC_2 OC_3 Area MaxVcy MaxREmp r_row d_row);
		Set Donors Imputed end=last;
		If last Then Call SymPut('DtlCnt2',_N_);
	Run;
	Data Imputed;
		Set Imputed;
		NewDistance=Round(Distance,0.001);
	Run;
	Proc Sort Data=Imputed; By SchNum Occ_Code JobID; Run;
	Title "ITEM IMPUTATION: Recepients and Donors";
	Proc Print Data=Imputed; Var SchNum Occ_Code JobID DonrSchd DonrOcc ImpLvl; Run;
	/******************************/
	data _null_;
		file _webout;
		put @1 '<br><a href = ' '"' 'http://oessun3/jvsest/index.html' '"' '><img src = ' '"' 'http://oessun3/sasweb/IntrNet8/images/back.JPG' '"' '></a>';
	run;
	/******************************/
	/******************************/
	ods html close;
	ods listing;
	/******************************/
%Mend ItemImpute;

/**************************\
End of JVS Item Imputation
\**************************/

/***************************\
Beginning of JVS Estimation
\***************************/

%Macro Estimation;
	Proc Sort Data=PermData.F_Estab Out=F_Estab; By SchNum CheckDig; Run;
	Proc Sort Data=PermData.F_Detail Out=F_Detail; By SchNum CheckDig; Run;
	Data Est;
		Merge F_Estab (In=In_Estab) F_Detail (In=In_Detail);
		By SchNum CheckDig;
        Est_Size="-";
		Est_Ind="------";
		Est_Own="-";
		Est_Geo="----";
		Est_JType="-";
		Est_JExp="-";
		Est_JEdu="-";
		Est_Occ="-------";
		If (Trim(Left("&MV_Size"))="A") Then Est_Size="0";
		Else If (Trim(Left("&MV_Size"))="S") Then Est_Size=Trim(Left(Size));
		If (Trim(Left("&MV_Ind"))="A") Then Est_Ind="000000";
		Else If (Trim(Left("&MV_Ind"))="S") Then Est_Ind=Trim(Left(Industry));
		If (Trim(Left("&MV_Own"))="A") Then Est_Own="0";
		Else If (Trim(Left("&MV_Own"))="S") Then Est_Own=Trim(Left(Owner));
		If (Trim(Left("&MV_Geo"))="A") Then Est_Geo="0000";
		Else If (Trim(Left("&MV_Geo"))="S") Then Do;
			If Length(Industry)=2 Then Est_Geo=SubStr(AllocStrata,5);
			Else If Length(Industry)=3 Then Est_Geo=SubStr(AllocStrata,6);
			Else If Length(Industry)=4 Then Est_Geo=SubStr(AllocStrata,7);
			Else If Length(Industry)=5 Then Est_Geo=SubStr(AllocStrata,8);
			Else If Length(Industry)=6 Then Est_Geo=SubStr(AllocStrata,9);
		End;
		If (Trim(Left("&MV_JType"))="A") Then Est_JType="0";
		Else If (Trim(Left("&MV_JType"))="L") Then Est_JType=Trim(Left(PartFull));
		If (Trim(Left("&MV_JExp"))="A") Then Est_JExp="0";
		Else If (Trim(Left("&MV_JExp"))="L") Then Est_JExp=Trim(Left(Exp_Lvl));
		If (Trim(Left("&MV_JEdu"))="A") Then Est_JEdu="0";
		Else If (Trim(Left("&MV_JEdu"))="L") Then Est_JEdu=Trim(Left(Edu_Lvl));
		If (In_Detail And Vac_N_Y="Y") Then Do;
			If (Trim(Left("&MV_Occ"))="A") Then Est_Occ="00-0000";
			Else If (Trim(Left("&MV_Occ"))="M") Then
			Est_Occ=SubStr(Trim(Left(Occ_Code)),1,2)||"-"||Repeat("0",4);
			Else If (Trim(Left("&MV_Occ"))="D") Then Est_Occ=Trim(Left(Occ_Code));
		End;
	Run;
	Proc Sort Data=Est Nodupkey Out=Est2; By Schnum; Run;
	Data Est2;
		Set Est2;
		Wgt=CurWgt*NRAF;
		Total_Emp=Wgt*Rpt_Emp;
	Run;
	Proc Sort Data=Est2; By Est_Geo Est_Ind Est_Size Est_Own; Run;
	Proc Summary Data=Est2;
		Var Total_Emp Wgt;
		By Est_Geo Est_Ind Est_Size Est_Own;
		Output Out=Estimates (Drop=_Type_ rename=(_Freq_=Rpt_n)) Sum=Total_Emp Sum_wgts;
	Run;
	Data Est2;
		Set Est;
		If Vacancies GE 0;
		WtdVacancies=CurWgt*NRAF*Vacancies;
	Run;
	Proc Sort Data=Est2; By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ; Run;
	Proc Summary Data=Est2;
		Var WtdVacancies;
		By Est_Geo Est_Ind Est_Size Est_Own;
		Output Out=TotVac (Keep=Est_Geo Est_Ind Est_Size Est_Own Total_Vacancies)
		Sum=Total_Vacancies;
	Run;
	Proc Summary Data=Est2;
		Var WtdVacancies;
		By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ;
		Output Out=OccVac (Keep=Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ Occ_Vac_Est _freq_ Rename=(_freq_=Rpt_Occ_n))
		Sum=Occ_Vac_Est;
	Run;
	Data Estimates;
		Merge Estimates (In=ln1) TotVac (In=ln2);
		By Est_Geo Est_Ind Est_Size Est_Own;
		Vacancy_Rate = Round(Total_Vacancies / Total_Emp, 0.0001);
	Run;
	Data Estimates;
		Merge Estimates (In=ln1) OccVac (In=ln2);
		By Est_Geo Est_Ind Est_Size Est_Own;
	Run;
	Proc Sort Data=PermData.Occ_Emp Out=Occ_Emp; By Est_Geo Est_Ind Est_Size Est_Own Est_Occ; Run;
	Proc Sort Data=PermData.TR_Data Out=TR_Data; By Est_Geo Est_Ind Est_Size Est_Own Est_Occ; Run;
	Data Both;
		Merge Occ_Emp (In=In_Emp) TR_Data (In=In_TR);
		By Est_Geo Est_Ind Est_Size Est_Own Est_Occ;
		T_Rate_h=T_Rate;
		Keep Est_Geo Est_Ind Est_Size Est_Own Est_Occ T_Rate_h OccEmp_Est;
	Run;
	Data TR_g;
		Set TR_Data;
		If Est_Occ="00-0000";
		T_Rate_g=T_Rate;
		Keep Est_Geo Est_Ind Est_Size Est_Own T_Rate_g;
	Run;
	Proc Sort Data=TR_g; By Est_Geo Est_Ind Est_Size Est_Own; Run;
	Data Both;
		Merge TR_g (In=In_1) Both (In=In_2);
		By Est_Geo Est_Ind Est_Size Est_Own;
	Run;
	Data TempE;
		Set Estimates;
		Keep Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ Vacancy_Rate Occ_Vac_Est;
	Run;
	Proc Sort Data=TempE; By Est_Geo Est_Ind Est_Size Est_Own Est_Occ; Run;
	Proc Sort Data=Both; By Est_Geo Est_Ind Est_Size Est_Own Est_Occ; Run;
	Data Both;
		Merge TempE (In=In_Est) Both (In=In_B);
		By Est_Geo Est_Ind Est_Size Est_Own Est_Occ;
		If In_Est;
		If ((OccEmp_Est NE .) And (OccEmp_Est) NE 0) Then Occ_Vac_Rate = Occ_Vac_Est / OccEmp_Est;
		Else Occ_Vac_Rate=.;
		If ((T_Rate_h NE .) And (T_Rate_h NE 0) And (T_Rate_g NE .) And (T_Rate_g NE 0) And (Vacancy_Rate NE .) And (Vacancy_Rate NE 0) And (Occ_Vac_Rate NE .)) Then Do;
			TAD_Est = (Occ_Vac_Rate / T_Rate_h) / (Vacancy_Rate / T_Rate_g);
		End;
		Else TAD_Est=.;
		Occ_Vac_Rate = Round(Occ_Vac_Rate,0.001);
		TAD_Est = Round(TAD_Est,0.001);
		Keep Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ Occ_Vac_Rate TAD_Est OccEmp_Est;
	Run;
	Data Estimates;
		Merge Estimates (In=In_Est) Both (In=In_B);
		By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ;
	Run;
	Data Est2;
		Set Est;
		If Vacancies GT 0;
		is_it_LT_30 = 0;
		is_it_30_59 = 0;
		is_it_60_998 = 0;
		is_it_Always = 0;
		If How_Long EQ "1" Then is_it_LT_30 = 1;
		Else If How_Long EQ "2" Then is_it_30_59 = 1;
		Else If How_Long EQ "3" Then is_it_60_998 = 1;
		Else If How_Long EQ "4" Then is_it_Always = 1;
		WTD_LT_30 = CurWgt*NRAF*is_it_LT_30*Vacancies;
		WTD_30_59 = CurWgt*NRAF*is_it_30_59*Vacancies;
		WTD_60_998 = CurWgt*NRAF*is_it_60_998*Vacancies;
		WTD_Always = CurWgt*NRAF*is_it_Always*Vacancies;
		If (PartFull = 'F') Then W_FullTime = CurWgt*NRAF*Vacancies;
	Run;
	Proc Sort Data = Est2; By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ; Run;
	Proc Summary Data = Est2;
		Var WTD_LT_30 WTD_30_59 WTD_60_998 WTD_Always W_FullTime;
		By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ;
		Output Out = Duration Sum = ;
	Run;
	Data Duration;
		Set Duration;
		By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ;
		Sum_Vacancies = Sum(WTD_LT_30,WTD_30_59,WTD_60_998,WTD_Always);
		TimeOpen_LT_30 = Round( WTD_LT_30/Sum_Vacancies, 0.001);
		TimeOpen_30_59 = Round( WTD_30_59/Sum_Vacancies, 0.001);
		TimeOpen_60_998 = Round( WTD_60_998/Sum_Vacancies, 0.001);
		TimeOpen_Always = Round( WTD_Always/Sum_Vacancies, 0.001);
		P_FullTime = Round( W_FullTime/Sum_Vacancies, 0.001);
		If P_FullTime = . Then P_FullTime = 0;
		P_PartTime = 1 - P_FullTime;
		Keep Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ TimeOpen_LT_30 TimeOpen_30_59 TimeOpen_60_998 TimeOpen_Always P_FullTime P_PartTime;
	Run;
	Proc Sort Data = Estimates; By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ; Run;
	Data Estimates;
		Merge Estimates Duration;
		By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ;
	Run;
	Data Est2;
		Set Est;
		If Vacancies > 0;
		V_N=0;
		V_H=0;
		V_V=0;
		V_2=0;
		V_B=0;
		V_A=0;
		Wtd_Perm=0;
		Wtd_TS=0;
		If Edu_lvl='N' Then V_N=1;
		Else If Edu_lvl='H' Then V_H=1;
		Else If Edu_lvl='V' Then V_V=1;
		Else If Edu_lvl='2' Then V_2=1;
		Else If Edu_lvl='B' Then V_B=1;
		Else If Edu_lvl='A' Then V_A=1;
		Wtd_V_N=CurWgt*NRAF*V_N*Vacancies;
		Wtd_V_H=CurWgt*NRAF*V_H*Vacancies;
		Wtd_V_V=CurWgt*NRAF*V_V*Vacancies;
		Wtd_V_2=CurWgt*NRAF*V_2*Vacancies;
		Wtd_V_B=CurWgt*NRAF*V_B*Vacancies;
		Wtd_V_A=CurWgt*NRAF*V_A*Vacancies;
		Wtd_Cert_License = CurWgt*NRAF*Cert_License*Vacancies;
		If (Perm_TS="P") Then Wtd_Perm = CurWgt*NRAF*Vacancies;
		Else If (Perm_TS="T") Then Wtd_TS = CurWgt*NRAF*Vacancies;
		Wtd_Denominator = CurWgt*NRAF*Vacancies;
	Run;
	Proc Sort Data=Est2; By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ; Run;
	Proc Summary Data=Est2;
		Var Wtd_V_N Wtd_V_H Wtd_V_V Wtd_V_2 Wtd_V_B Wtd_V_A Wtd_Cert_License Wtd_Denominator Wtd_Perm Wtd_TS;
		By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ;
		Output Out=Edu Sum= ;
	Run;
	Data Edu;
		Set Edu;
		By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ;
		Edu_P1_Est=Round(Wtd_V_N/Wtd_Denominator,0.001);
		Edu_P2_Est=Round(Wtd_V_H/Wtd_Denominator,0.001);
		Edu_P3_Est=Round(Wtd_V_V/Wtd_Denominator,0.001);
		Edu_P4_Est=Round(Wtd_V_2/Wtd_Denominator,0.001);
		Edu_P5_Est=Round(Wtd_V_B/Wtd_Denominator,0.001);
		Edu_P6_Est=Round(Wtd_V_A/Wtd_Denominator,0.001);
		Lic_Req_P_Est = Round(Wtd_Cert_License/Wtd_Denominator,0.001);
		Perm_Est_P = Round(Wtd_Perm/Wtd_Denominator,0.001);
		TS_Est_P = Round(Wtd_TS/Wtd_Denominator,0.001);
		Keep Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ Edu_P1_Est Edu_P2_Est Edu_P3_Est Edu_P4_Est Edu_P5_Est Edu_P6_Est Lic_Req_P_Est Perm_Est_P TS_Est_P;
	Run;
	Proc Sort Data=Estimates; By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ; Run;
	Data Estimates;
		Merge Estimates Edu;
		By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ;
	Run;
	Data Est2;
		Set Est;
		If Vacancies > 0;
		L_N=0;
		L_W=0;
		L_R=0;
		If Exp_lvl='N' Then L_N=1;
		Else If Exp_lvl='W' Then L_W=1;
		Else If Exp_lvl='R' Then L_R=1;
		Wtd_L_N=CurWgt*NRAF*L_N*Vacancies;
		Wtd_L_W=CurWgt*NRAF*L_W*Vacancies;
		Wtd_L_R=CurWgt*NRAF*L_R*Vacancies;
	Run;
	Proc Sort Data=Est2; By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ; Run;
	Proc Summary Data=Est2;
		Var Wtd_L_N Wtd_L_W Wtd_L_R;
		By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ;
		Output Out=Exp (Keep=Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ Wtd_L_N Wtd_L_W Wtd_L_R) Sum= ;
	Run;
	Data Exp;
		Set Exp;
		By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ;
		Denominator=Wtd_L_N+Wtd_L_W+Wtd_L_R;
		Exp_P1_Est=Wtd_L_N/Denominator;
		Exp_P2_Est=Wtd_L_W/Denominator;
		Exp_P3_Est=Wtd_L_R/Denominator;
		Keep Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ Exp_P1_Est Exp_P2_Est Exp_P3_Est;
	Run;
	Proc Sort Data=Estimates; By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ; Run;
	Data Estimates;
		Merge Estimates Exp;
		By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ;
	Run;
	Data Est2;
		Set Est;
		If Vacancies GT 0;
		Wtd_Vacancies=CurWgt*NRAF*Vacancies;
		Wage = (1/2)*(Min_Wage+Max_Wage);
		Numerator=Wtd_Vacancies*Wage;
		Denominator=Wtd_Vacancies;
	Run;
	Proc Sort Data=Est2; By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ; Run;
	Proc Summary Data=Est2;
		Var Numerator Denominator;
		By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ;
		Output Out=PreWage (Keep=Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ Numerator Denominator)
		Sum=Numerator Denominator;
	Run;
	Data PreWage;
		Set PreWage;
		By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ;
		Wage_Est_Mu=Round(Numerator/Denominator,0.001);
		Keep Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ Wage_Est_Mu;
	Run;
	Proc Univariate Data=Est2 NoPrint;
		Var Min_Wage Max_Wage;
		By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ;
		Output Out=MinMaxWage Min=Wage_Est_Min Max=Wage_Est_Max;
	Run;
	Data Estimates;
		Merge Estimates (In=ln1) PreWage (In=ln2) MinMaxWage (In=ln3);
		By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JExp Est_JEdu Est_Occ;
	Run;
	Data Est2;
		Set Est;
		If Vacancies GT 0;
		If (Benefit1=1) Then Q_H_1=Vacancies; Else Q_H_1=0;
		If (Benefit2=1) Then Q_H_2=Vacancies; Else Q_H_2=0;
		If (Benefit3=1) Then Q_H_3=Vacancies; Else Q_H_3=0;
		If (Benefit4=1) Then Q_H_4=Vacancies; Else Q_H_4=0;
		If (Benefit5=1) Then Q_H_5=Vacancies; Else Q_H_5=0;
		If (Benefit6=1) Then Q_H_6=Vacancies; Else Q_H_6=0;
		WQ_H1=CurWgt*NRAF*Q_H_1;
		WQ_H2=CurWgt*NRAF*Q_H_2;
		WQ_H3=CurWgt*NRAF*Q_H_3;
		WQ_H4=CurWgt*NRAF*Q_H_4;
		WQ_H5=CurWgt*NRAF*Q_H_5;
		WQ_H6=CurWgt*NRAF*Q_H_6;
		Keep Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JEdu Est_JExp Est_Occ WQ_H1 WQ_H2 WQ_H3 WQ_H4 WQ_H5 WQ_H6;
	Run;
	Proc Sort Data=Est2; By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JEdu Est_JExp Est_Occ; Run;
	Proc Summary Data=Est2;
		Var WQ_H1 WQ_H2 WQ_H3 WQ_H4 WQ_H5 WQ_H6;
		By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JEdu Est_JExp Est_Occ;
		Output Out=BenSum (Drop=_Type_ _Freq_) Sum=;
	Run;
	Data TempEst;
		Set Estimates;
		Keep Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JEdu Est_JExp Est_Occ Occ_Vac_Est;
	Run;
	Proc Sort Data=TempEst; By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JEdu Est_JExp Est_Occ; Run;
	Data BenSum;
		Merge TempEst (In=In1) BenSum (In=In2);
		By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JEdu Est_JExp Est_Occ;
	Run;
	Data BenSum;
		Set BenSum;
		By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JEdu Est_JExp Est_Occ;
		P_H1_Est = Round(WQ_H1 / Occ_Vac_Est,0.001);
		P_H2_Est = Round(WQ_H2 / Occ_Vac_Est,0.001);
		P_H3_Est = Round(WQ_H3 / Occ_Vac_Est,0.001);
		P_H4_Est = Round(WQ_H4 / Occ_Vac_Est,0.001);
		P_H5_Est = Round(WQ_H5 / Occ_Vac_Est,0.001);
		P_H6_Est = Round(WQ_H6 / Occ_Vac_Est,0.001);
		Keep Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JEdu Est_JExp Est_Occ P_H1_Est P_H2_Est P_H3_Est P_H4_Est P_H5_Est P_H6_Est;
	Run;
	Proc Sort Data=Estimates; By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JEdu Est_JExp Est_Occ; Run;
	Data Estimates;
		Merge Estimates (In=In1) BenSum (In=In2);
		By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JEdu Est_JExp Est_Occ;
	Run;
	Data Est2;
		Set Est;
		If Vacancies GT 0;
		Keep Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JEdu Est_JExp Est_Occ Vacancies;
	Run;
	Data TempEst;
		Set Estimates;
		Keep Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JEdu Est_JExp Est_Occ Rpt_Occ_n Occ_Vac_Est;
	Run;
	Proc Sort Data=Est2; By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JEdu Est_JExp Est_Occ; Run;
	Proc Sort Data=TempEst; By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JEdu Est_JExp Est_Occ; Run;
	Data Est3;
		Merge TempEst (In=In1) Est2 (In=In2);
		By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JEdu Est_JExp Est_Occ;
	Run;
	Proc Sort Data=Est3; By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JEdu Est_JExp Est_Occ Descending Vacancies; Run;
	Data Est4;
		Set Est3;
		By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JEdu Est_JExp Est_Occ Descending Vacancies;
		Retain Rule1 Rule2 Rule3 Sum2Vac;
		If First.Est_Occ Then Do;
			Rule1="FAIL";
			Rule2="FAIL";
			Rule3="FAIL";
			Counter=0;
			Sum2Vac=0;
			If Rpt_Occ_n GE 3 Then Rule1="PASS";
		End;
		Counter+1;
		If (Counter=1) Then Do;
			If (Vacancies LT (0.5*Occ_Vac_Est)) Then Rule2="PASS";
			Sum2Vac=Vacancies;
		End;
		If (Counter=2) Then Do;
			Sum2Vac=Sum2Vac+Vacancies;
			If (Sum2Vac LT (0.75*Occ_Vac_Est)) Then Rule3="PASS";
		End;
		If Last.Est_Occ Then Do;
			Confidential="PASS";
			If ((Rule1="FAIL") Or (Rule2="FAIL") Or (Rule3="FAIL")) Then Confidential="FAIL";
			Output;
		End;
		Keep Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JEdu Est_JExp Est_Occ Confidential;
	Run;

	Proc Sort Data=Est4; By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JEdu Est_JExp Est_Occ Confidential; Run;
	Proc Sort Data=Estimates; By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JEdu Est_JExp Est_Occ; Run;
	Data Estimates;
		Merge Estimates (In=In1) Est4 (In=In2);
		By Est_Geo Est_Ind Est_Size Est_Own Est_JType Est_JEdu Est_JExp Est_Occ;
	Run;
	Data PermEst.&EstLevel;
		Set Estimates;
		Format Confidential $4.
               Edu_P1_Est 8.3
               Edu_P2_Est 8.3
               Edu_P3_Est 8.3
               Edu_P4_Est 8.3
               Edu_P5_Est 8.3
               Edu_P6_Est 8.3
               Est_Geo $6.
               Est_Ind $6.
               Est_JEdu $1.
               Est_JExp $1.
               Est_JType $1.
               Est_Occ $7.
               Est_Own $1.
               Est_Size $1.
               Occ_Vac_Est 9.2
               Occ_Vac_Rate 9.3
               OccEmp_Est 9.1
               P_H1_Est 8.3
               P_H2_Est 8.3
               P_H3_Est 8.3
               P_H4_Est 8.3
               P_H5_Est 8.3
               P_H6_Est 8.3
               Rpt_Occ_n 8.
               Rpt_n 8.
               Sum_wgts 8.1
               TAD_Est 9.2
               TimeOpen_30_59 8.3
               TimeOpen_60_998 8.3
               TimeOpen_Always 8.3
               TimeOpen_LT_30 8.3
               Total_Emp 9.1
               Total_Vacancies 9.1
               Vacancy_Rate 8.3
               Wage_Est_Max 8.2
               Wage_Est_Min 8.2
               Wage_Est_Mu 8.2
               TS_Est_P 8.3
               Perm_Est_P 8.3
               P_FullTime 8.3
               P_PartTime 8.3;
	Run;
	Proc Export Data=PermEst.&EstLevel OutFile="&outputlocation/&EstLevel..dbf" DBMS=DBF Replace; Run;
	/******************************/
	ods listing close;
	ods html body=_webout(dynamic) rs=none style=styles.format2;
	/******************************/
	Title "ESTIMATES: &EstLevel";
	Proc Print Data=Estimates; Run;
	/******************************/
	data _null_;
		file _webout;
		put @1 '<br><a href = ' '"' 'http://oessun3/jvsest/index.html' '"' '><img src = ' '"' 'http://oessun3/sasweb/IntrNet8/images/back.JPG' '"' '></a>';
	run;
	/******************************/
	/******************************/
	ods html close;
	ods listing;
	/******************************/
%Mend Estimation;

/*********************\
End of JVS Estimation
\*********************/

/***************************\
Beginning of JVS Influences
\***************************/

%Macro Influence;
	Proc Sort Data=PermData.F_Estab Out=F_Estab; By SchNum CheckDig; Run;
	Proc Sort Data=PermData.F_Detail Out=F_Detail; By SchNum CheckDig; Run;
	Data Influences1;
		Merge F_Estab (In=In1) F_Detail (In=In2);
		By SchNum CheckDig;
		AllocSize="-";
		AllocInd="------";
		AllocOwn="-";
		AllocGeo="----";
		If (Trim(Left("&MV_Size"))="A") Then AllocSize="0";
		Else If (Trim(Left("&MV_Size"))="S") Then AllocSize=Trim(Left(Size));
		If (Trim(Left("&MV_Ind"))="A") Then AllocInd="000000";
		Else If (Trim(Left("&MV_Ind"))="S") Then AllocInd=Trim(Left(Industry));
		If (Trim(Left("&MV_Own"))="A") Then AllocOwn="0";
		Else If (Trim(Left("&MV_Own"))="S") Then AllocOwn=Trim(Left(Owner));
		If (Trim(Left("&MV_Geo"))="A") Then AllocGeo="0000";
		Else If (Trim(Left("&MV_Geo"))="S") Then Do;
			If Length(Industry)=2 Then AllocGeo=SubStr(AllocStrata,5);
			Else If Length(Industry)=3 Then AllocGeo=SubStr(AllocStrata,6);
			Else If Length(Industry)=4 Then AllocGeo=SubStr(AllocStrata,7);
			Else If Length(Industry)=5 Then AllocGeo=SubStr(AllocStrata,8);
			Else If Length(Industry)=6 Then AllocGeo=SubStr(AllocStrata,9);
		End;
		EmpEst=CurWgt*NRAF*Rpt_Emp;
		EmpFlag=0;
		VacEst=CurWgt*NRAF*Vacancies;
		VacFlag=0;
		InfCell=Trim(Left(Trim(Left(AllocSize))||Trim(Left(AllocInd))||Trim(Left(AllocOwn))||Trim(Left(AllocGeo))));
	Run;
	Proc Sort Data=Influences1; By InfCell; Run;
	Proc Summary Data=Influences1;
		Var EmpEst VacEst;
		By InfCell;
		Output Out=Influences2 (Drop=_Type_ _Freq_) Sum=TEmpEst TVacEst;
	Run;
	Proc Sort Data=Influences2; By InfCell; Run;
	Data Influences3;
		Merge Influences1 (In=In1) Influences2 (In=In2);
		By InfCell;
	Run;
	Data Influences4;
		Set Influences3;
		EmpInf=EmpEst/TEmpEst;
		If EmpInf>&cut1 Then EmpFlag=1;
		VacInf=VacEst/TVacEst;
		If VacInf>&cut2 Then VacFlag=1;
	Run;
	Data Influences;
		Set Influences4;
		EmpCut=&cut1;
		VacCut=&cut2;
		If (EmpFlag=1 or VacFlag=1) Then Output;
	Run;
	Proc Sort Data=Influences; By AllocStrata; Run;
	/******************************/
	ods listing close;
	ods html body=_webout(dynamic) rs=none style=styles.format2;
	/******************************/
	Title "INFLUENCES";
	Proc Print Data=Influences; Var AllocStrata SchNum CheckDig JobID EmpEst EmpFlag EmpInf EmpCut VacEst VacFlag VacInf VacCut; Run;
	/******************************/
	data _null_;
		file _webout;
		put @1 '<br><a href = ' '"'
		'http://oessun3/sasweb/IntrNet8/jvsest/runinfluence.html' '"' '><img src = ' '"' 'http://oessun3/sasweb/IntrNet8/images/back.JPG' '"' '></a>';
	run;
	/******************************/
	/******************************/
	ods html close;
	ods listing;
	/******************************/
%Mend Influence;

/*********************\
End of JVS Influences
\*********************/

/***************************\
Beginning of JVS Extraction
\***************************/

%Macro Extraction;
	%If &Cell^=' ' %Then %Do;
		Proc Sort Data=PermData.F_Estab Out=F_Estab; By SchNum CheckDig; Run;
		Proc Sort Data=PermData.F_Detail Out=F_Detail; By SchNum CheckDig; Run;
		Data Est;
			Merge F_Estab (In=In1) F_Detail (In=In2);
			By SchNum CheckDig;
			Est_Size="-";
			Est_Ind="------";
			Est_Own="-";
			Est_Geo="----";
			Est_JType="-";
			Est_JExp="-";
			Est_JEdu="-";
			Est_Occ="-------";
			If (Trim(Left("&MV_Size"))="A") Then Est_Size="0";
			Else If (Trim(Left("&MV_Size"))="S") Then Est_Size=Trim(Left(Size));
			If (Trim(Left("&MV_Ind"))="A") Then Est_Ind="000000";
			Else If (Trim(Left("&MV_Ind"))="S") Then Est_Ind=Trim(Left(Industry));
			If (Trim(Left("&MV_Own"))="A") Then Est_Own="0";
			Else If (Trim(Left("&MV_Own"))="S") Then Est_Own=Trim(Left(Owner));
			If (Trim(Left("&MV_Geo"))="A") Then Est_Geo="0000";
			Else If (Trim(Left("&MV_Geo"))="S") Then Do;
				If Length(Industry)=2 Then Est_Geo=SubStr(AllocStrata,5);
				Else If Length(Industry)=3 Then Est_Geo=SubStr(AllocStrata,6);
				Else If Length(Industry)=4 Then Est_Geo=SubStr(AllocStrata,7);
				Else If Length(Industry)=5 Then Est_Geo=SubStr(AllocStrata,8);
				Else If Length(Industry)=6 Then Est_Geo=SubStr(AllocStrata,9);
			End;
			If (Trim(Left("&MV_JType"))="A") Then Est_JType="0";
			Else If (Trim(Left("&MV_JType"))="L") Then Est_JType=Trim(Left(PartFull));
			If (Trim(Left("&MV_JExp"))="A") Then Est_JExp="0";
			Else If (Trim(Left("&MV_JExp"))="L") Then Est_JExp=Trim(Left(Exp_Lvl));
			If (Trim(Left("&MV_JEdu"))="A") Then Est_JEdu="0";
			Else If (Trim(Left("&MV_JEdu"))="L") Then Est_JEdu=Trim(Left(Edu_Lvl));
			If (In_Detail And Vac_N_Y="Y") Then Do;
				If (Trim(Left("&MV_Occ"))="A") Then Est_Occ="00-0000";
				Else If (Trim(Left("&MV_Occ"))="M") Then
				Est_Occ=SubStr(Trim(Left(Occ_Code)),1,2)||"-"||Repeat("0",4);
				Else If (Trim(Left("&MV_Occ"))="D") Then Est_Occ=Trim(Left(Occ_Code));
			End;
		Run;
		Data Extract (Keep=SchNum CheckDig JobID AllocStrata NRAF CurWgt Rpt_Emp Vacancies EmpEst VacEst);
		Set Est;
			EmpEst=NRAF*CurWgt*Rpt_Emp;
			VacEst=NRAF*CurWgt*Vacancies;
			If AllocStrata="&Cell" Then Output;
		Run;
		Proc Sort Data=Extract; By SchNum CheckDig JobID; Run;
		/******************************/
		ods listing close;
		ods html body=_webout(dynamic) rs=none style=styles.format2;
		/******************************/
		Title "CELL EXTRACTION: &Cell";
		Proc Print Data=Extract; Var SchNum CheckDig JobID NRAF CurWgt Rpt_Emp Vacancies EmpEst VacEst; Run;
		/******************************/
		data _null_;
			file _webout;
			put @1 '<br><a href = ' '"' 'http://oessun3/sasweb/IntrNet8/jvsest/runinfluence.html' '"' '><img src = ' '"' 'http://oessun3/sasweb/IntrNet8/images/back.JPG' '"' '></a>';
		run;
		/******************************/
		/******************************/
		ods html close;
		ods listing;
		/******************************/
	%End;
%Mend Extraction;

/*********************\
End of JVS Extraction
\*********************/

/************************************\
Beginning of JVS Variance Estimation
\************************************/

%Macro Variance;
	Data Variance1_1;
		Set PermData.SampleID;
		AllocStrata=Allocation_Strata;
	Run;
	Proc Sort Data=Variance1_1; By AllocStrata; Run;
	Proc Summary Data=Variance1_1;
		Var Weight;
		By AllocStrata;
		Output Out=Variance1_2 (Drop=_Type_ _Freq_) Sum=Population;
	Run;
	Proc Summary Data=Variance1_1;
		By AllocStrata;
		Output Out=Variance1_3 (Drop=_Type_ ReName=_Freq_=Sample);
	Run;
	Proc Sort Data=PermData.F_Estab Out=Variance2_1; By AllocStrata; Run;
	Proc Summary Data=Variance2_1;
		By AllocStrata;
		Output Out=Variance2_2 (Drop=_Type_ ReName=_Freq_=Respondents);
	Run;
	Data Variance3_1;
		Merge Variance1_2 (In=In1) Variance1_3 (In=In2) Variance2_2 (In=In3);
		By AllocStrata;
		If Respondents=. Then Respondents=0;
	Run;
	Proc Sort Data=PermData.Estab Out=Estab; By SchNum CheckDig; Run;
	Proc Sort Data=PermData.F_Estab Out=F_Estab; By SchNum CheckDig; Run;
	Data Variance4_1;
		Merge Estab (In=In1) F_Estab (In=In2);
		By SchNum CheckDig;
	Run;
	Data Variance4_2 (Keep=SchNum CheckDig AllocStrata Est_Geo Est_Ind Est_Size Est_Own Wgt Adj_Emp Emp_Est);
		Set Variance4_1;
		Est_Size="-";
		Est_Ind="------";
		Est_Own="-";
		Est_Geo="----";
		If (Trim(Left("&MV_Size"))="A") Then Est_Size="0";
		Else If (Trim(Left("&MV_Size"))="S") Then Est_Size=SubStr(AllocStrata,1,1);
		If (Trim(Left("&MV_Ind"))="A") Then Est_Ind="000000";
		Else If (Trim(Left("&MV_Ind"))="S") Then Est_Ind=Trim(Left(Industry));
		If (Trim(Left("&MV_Own"))="A") Then Est_Own="0";
		Else If (Trim(Left("&MV_Own"))="S") Then Est_Own=Trim(Left(Owner));
		If (Trim(Left("&MV_Geo"))="A") Then Est_Geo="0000";
		Else If (Trim(Left("&MV_Geo"))="S") Then Do;
			If Length(Industry)=2 Then Est_Geo=SubStr(AllocStrata,5);
			Else If Length(Industry)=3 Then Est_Geo=SubStr(AllocStrata,6);
			Else If Length(Industry)=4 Then Est_Geo=SubStr(AllocStrata,7);
			Else If Length(Industry)=5 Then Est_Geo=SubStr(AllocStrata,8);
			Else If Length(Industry)=6 Then Est_Geo=SubStr(AllocStrata,9);
		End;
		Wgt=NRAF*CurWgt;
		If Wgt=. Then Wgt=0;
		Adj_Emp=NRAF*Rpt_Emp;
		If Adj_Emp=. Then Adj_Emp=0;
		Emp_Est=Wgt*Rpt_Emp;
		If Emp_Est=. Then Emp_Est=0;
	Run;
	Proc Sort Data=Variance4_2; By AllocStrata; Run;
	Proc Summary Data=Variance4_2;
		Var Wgt Emp_Est;
		By AllocStrata;
		Output Out=Variance4_3 (Drop=_Type_ _Freq_) Sum=Adj_Population EmpEst;
	Run;
	Proc Summary Data=Variance4_2;
		Var Adj_Emp;
		By AllocStrata;
		Output Out=Variance4_4 (Drop=_Type_ _Freq_) StdDev=StdDev;
	Run;
	Data Variance4_5;
		Merge Variance3_1 (In=In1) Variance4_2 (In=In2) Variance4_3 (In=In3) Variance4_4 (In=In4);
		By AllocStrata;
		If Respondents=0 Then EmpEst=.;
		If Respondents=0 Then StdDev=.;
		If Respondents>0 Then Do;
		FPC=(Adj_Population-Respondents)/Adj_Population;
		If FPC=0 Then FPC=(Adj_Population-(Respondents-1))/Adj_Population;
		SE_EmpEst=SQRT((((Adj_Population**2)*(StdDev**2))/Respondents)*FPC);
		End;
		Else SE_EmpEst=.;
		If Adj_Population=. Then Adj_Population=0;
	Run;
	Proc Sort Data=Variance4_5 NoDupKey Out=Variance4_6; By AllocStrata; Run;
	Proc Sort Data=Variance4_6; By Est_Geo Est_Ind Est_Size Est_Own; Run;
	Proc Summary Data=Variance4_6;
		Var Population Sample Respondents Adj_Population EmpEst StdDev SE_EmpEst;
		By Est_Geo Est_Ind Est_Size Est_Own;
		Output Out=Variance4_7 (Drop=_Type_ _Freq_) Sum=;
	Run;
	/******************************/
	ods listing close;
	ods html body=_webout(dynamic) rs=none style=styles.format2;
	/******************************/
	Title "EMPLOYMENT STATISTICS: &EstLevel";
	Proc Print Data=Variance4_7; Var Est_Geo Est_Ind Est_Size Est_Own Population Adj_Population Sample Respondents EmpEst SE_EmpEst StdDev; Run;
	Proc Sort Data=PermData.F_Detail Out=F_Detail; By SchNum CheckDig; Run;
	Data Variance5_1;
		Merge Estab (In=In1) F_Estab (In=In2) F_Detail (In=In3);
		By SchNum CheckDig;
	Run;
	Data Variance5_2 (Keep=SchNum CheckDig AllocStrata Est_Geo Est_Ind Est_Size Est_Own Adj_Vac Vac_Est);
		Set Variance5_1;
		Est_Size="-";
		Est_Ind="------";
		Est_Own="-";
		Est_Geo="----";
		If (Trim(Left("&MV_Size"))="A") Then Est_Size="0";
		Else If (Trim(Left("&MV_Size"))="S") Then Est_Size=SubStr(AllocStrata,1,1);
		If (Trim(Left("&MV_Ind"))="A") Then Est_Ind="000000";
		Else If (Trim(Left("&MV_Ind"))="S") Then Est_Ind=Trim(Left(Industry));
		If (Trim(Left("&MV_Own"))="A") Then Est_Own="0";
		Else If (Trim(Left("&MV_Own"))="S") Then Est_Own=Trim(Left(Owner));
		If (Trim(Left("&MV_Geo"))="A") Then Est_Geo="0000";
		Else If (Trim(Left("&MV_Geo"))="S") Then Do;
			If Length(Industry)=2 Then Est_Geo=SubStr(AllocStrata,5);
			Else If Length(Industry)=3 Then Est_Geo=SubStr(AllocStrata,6);
			Else If Length(Industry)=4 Then Est_Geo=SubStr(AllocStrata,7);
			Else If Length(Industry)=5 Then Est_Geo=SubStr(AllocStrata,8);
			Else If Length(Industry)=6 Then Est_Geo=SubStr(AllocStrata,9);
		End;
		Wgt=NRAF*CurWgt;
		If Wgt=. Then Wgt=0;
		Adj_Vac=NRAF*Vacancies;
		If Adj_Vac=. Then Adj_Vac=0;
		Vac_Est=Wgt*Vacancies;
		If Vac_Est=. Then Vac_Est=0;
	Run;
	Proc Sort Data=Variance5_2; By AllocStrata; Run;
	Proc Summary Data=Variance5_2;
		Var Vac_Est;
		By AllocStrata;
		Output Out=Variance5_3 (Drop=_Type_ _Freq_) Sum=VacEst;
	Run;
	Proc Summary Data=Variance5_2;
		Var Adj_Vac;
		By AllocStrata;
		Output Out=Variance5_4 (Drop=_Type_ _Freq_) StdDev=StdDev;
	Run;
	Data Variance5_5 (Drop=EmpEst); Set Variance4_3; Run;
	Data Variance5_6;
	Merge Variance3_1 (In=In1) Variance5_2 (In=In2) Variance5_3 (In=In3) Variance5_4 (In=In4) Variance5_5 (In=In5);
		By AllocStrata;
		If Respondents=0 Then VacEst=.;
		If Respondents=0 Then StdDev=.;
		If Respondents>0 Then Do;
		FPC=(Adj_Population-Respondents)/Adj_Population;
		If FPC=0 Then FPC=(Adj_Population-(Respondents-1))/Adj_Population;
		SE_VacEst=SQRT((((Adj_Population**2)*(StdDev**2))/Respondents)*FPC);
		End;
		Else SE_VacEst=.;
		If Adj_Population=. Then Adj_Population=0;
	Run;
	Proc Sort Data=Variance5_6 NoDupKey Out=Variance5_7; By AllocStrata; Run;
	Proc Sort Data=Variance5_7; By Est_Geo Est_Ind Est_Size Est_Own; Run;
	Proc Summary Data=Variance5_7;
		Var Population Sample Respondents Adj_Population VacEst StdDev SE_VacEst;
		By Est_Geo Est_Ind Est_Size Est_Own;
		Output Out=Variance5_8 (Drop=_Type_ _Freq_) Sum=;
	Run;
	Title "VACANCY STATISTICS: &EstLevel";
	Proc Print Data=Variance5_8; Var Est_Geo Est_Ind Est_Size Est_Own Population Adj_Population Sample Respondents VacEst SE_VacEst StdDev; Run;
	/******************************/
	data _null_;
		file _webout;
		put @1 '<br><a href = ' '"' 'http://oessun3/jvsest/index.html' '"' '><img src = ' '"' 'http://oessun3/sasweb/IntrNet8/images/back.JPG' '"' '></a>';
	run;
	/******************************/
	/******************************/
	ods html close;
	ods listing;
	/******************************/
%Mend Variance;

/******************************\
End of JVS Variance Estimation
\******************************/