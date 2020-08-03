-- READY DEMOGRAPHIC AND GEOGRAPHIC ATTRIBUTES
-- Step1: Obtain select fields from the Acxiom PAF table, adding population density.
-- https://api.qubole.com/v2/analyze?command_id=136442394
set hive.auto.convert.join = true;
set hive.exec.compress.intermediate = true;
set hive.execution.engine = mr;
set hive.map.aggr.hash.percentmemory = 0.125;
set hive.vectorized.execution.enabled = true;
set mapreduce.jobtracker.expire.trackers.interval = 1800000;
set mapreduce.map.java.opts = -Xmx3072m;
set mapreduce.map.memory.mb = 4096;
set mapreduce.reduce.java.opts = -Xmx6144m;
set mapreduce.reduce.memory.mb = 8192;
set mapreduce.task.timeout = 1800000;

drop table if exists default.jwillingham_household_demographics_step1;

create table default.jwillingham_household_demographics_step1 as
select consumer_pel,
       household_pel,
       zip zip_code,
       fips_county,
       state_abbreviation,
       county_name,
       description population_density_description,
       rural_urban_county_code_2013 population_density_code,
       case
         when `18633` = '1' then 'Abilene-Sweetwater'
         when `18634` = '1' then 'Albany, GA'
         when `18635` = '1' then 'Albany-Schenectady-Troy'
         when `18636` = '1' then 'Albuquerque-Santa Fe'
         when `18637` = '1' then 'Alexandria, LA'
         when `18638` = '1' then 'Alpena'
         when `18639` = '1' then 'Amarillo'
         when `18640` = '1' then 'Anchorage'
         when `18641` = '1' then 'Atlanta'
         when `18642` = '1' then 'Augusta-Aiken'
         when `18643` = '1' then 'Austin'
         when `18644` = '1' then 'Bakersfield'
         when `18645` = '1' then 'Baltimore'
         when `18646` = '1' then 'Bangor'
         when `18647` = '1' then 'Baton Rouge'
         when `18648` = '1' then 'Beaumont-Port Arthur'
         when `18649` = '1' then 'Bend, OR'
         when `18650` = '1' then 'Billings'
         when `18651` = '1' then 'Biloxi-Gulfport'
         when `18652` = '1' then 'Binghamton'
         when `18653` = '1' then 'Birmingham (Ann And Tusc)'
         when `18654` = '1' then 'Bluefield-Beckley-Oak Hill'
         when `18655` = '1' then 'Boise'
         when `18656` = '1' then 'Boston (Manchester)'
         when `18657` = '1' then 'Bowling Green'
         when `18658` = '1' then 'Buffalo'
         when `18659` = '1' then 'Burlington-Plattsburgh'
         when `18660` = '1' then 'Butte-Bozeman'
         when `18661` = '1' then 'Casper-Riverton'
         when `18662` = '1' then 'Cedar Rapids-Wtrlo-Iwc&Dub'
         when `18663` = '1' then 'Champaign&Sprngfld-Decatur'
         when `18664` = '1' then 'Charleston, SC'
         when `18665` = '1' then 'Charleston-Huntington'
         when `18666` = '1' then 'Charlotte'
         when `18667` = '1' then 'Charlottesville'
         when `18668` = '1' then 'Chattanooga'
         when `18669` = '1' then 'Cheyenne-Scottsbluff'
         when `18670` = '1' then 'Chicago'
         when `18671` = '1' then 'Chico-Redding'
         when `18672` = '1' then 'Cincinnati'
         when `18673` = '1' then 'Clarksburg-Weston'
         when `18674` = '1' then 'Cleveland-Akron (Canton)'
         when `18675` = '1' then 'Colorado Springs-Pueblo'
         when `18676` = '1' then 'Columbia, SC'
         when `18677` = '1' then 'Columbia-Jefferson City'
         when `18678` = '1' then 'Columbus, GA (Opelika, AL)'
         when `18679` = '1' then 'Columbus, OH'
         when `18680` = '1' then 'Columbus-Tupelo-W Pnt-Hstn'
         when `18681` = '1' then 'Corpus Christi'
         when `18682` = '1' then 'Dallas-Ft. Worth'
         when `18683` = '1' then 'Davenport-R.Island-Moline'
         when `18684` = '1' then 'Dayton'
         when `18685` = '1' then 'Denver'
         when `18686` = '1' then 'Des Moines-Ames'
         when `18687` = '1' then 'Detroit'
         when `18688` = '1' then 'Dothan'
         when `18689` = '1' then 'Duluth-Superior'
         when `18690` = '1' then 'El Paso (Las Cruces)'
         when `18691` = '1' then 'Elmira (Corning)'
         when `18692` = '1' then 'Erie'
         when `18693` = '1' then 'Eugene'
         when `18694` = '1' then 'Eureka'
         when `18695` = '1' then 'Evansville'
         when `18696` = '1' then 'Fairbanks'
         when `18697` = '1' then 'Fargo-Valley City'
         when `18698` = '1' then 'Flint-Saginaw-Bay City'
         when `18699` = '1' then 'Fresno-Visalia'
         when `18700` = '1' then 'Ft. Myers-Naples'
         when `18701` = '1' then 'Ft. Smith-Fay-Sprngdl-Rgrs'
         when `18702` = '1' then 'Ft. Wayne'
         when `18703` = '1' then 'Gainesville'
         when `18704` = '1' then 'Glendive'
         when `18705` = '1' then 'Grand Junction-Montrose'
         when `18706` = '1' then 'Grand Rapids-Kalmzoo-B.Crk'
         when `18707` = '1' then 'Great Falls'
         when `18708` = '1' then 'Green Bay-Appleton'
         when `18709` = '1' then 'Greensboro-H.Point-W.Salem'
         when `18710` = '1' then 'Greenville-N.Bern-Washngtn'
         when `18711` = '1' then 'Greenvll-Spart-Ashevll-And'
         when `18712` = '1' then 'Greenwood-Greenville'
         when `18713` = '1' then 'Harlingen-Wslco-Brnsvl-Mca'
         when `18714` = '1' then 'Harrisburg-Lncstr-Leb-York'
         when `18715` = '1' then 'Harrisonburg'
         when `18716` = '1' then 'Hartford & New Haven'
         when `18717` = '1' then 'Hattiesburg-Laurel'
         when `18718` = '1' then 'Helena'
         when `18719` = '1' then 'Honolulu'
         when `18720` = '1' then 'Houston'
         when `18721` = '1' then 'Huntsville-Decatur (Flor)'
         when `18722` = '1' then 'Idaho Fals-Pocatllo(Jcksn)'
         when `18723` = '1' then 'Indianapolis'
         when `18724` = '1' then 'Jackson, MS'
         when `18725` = '1' then 'Jackson, TN'
         when `18726` = '1' then 'Jacksonville'
         when `18727` = '1' then 'Johnstown-Altoona-St Colge'
         when `18728` = '1' then 'Jonesboro'
         when `18729` = '1' then 'Joplin-Pittsburg'
         when `18730` = '1' then 'Juneau'
         when `18731` = '1' then 'Kansas City'
         when `18732` = '1' then 'Knoxville'
         when `18733` = '1' then 'La Crosse-Eau Claire'
         when `18734` = '1' then 'Lafayette, IN'
         when `18735` = '1' then 'Lafayette, LA'
         when `18736` = '1' then 'Lake Charles'
         when `18737` = '1' then 'Lansing'
         when `18738` = '1' then 'Laredo'
         when `18739` = '1' then 'Las Vegas'
         when `18740` = '1' then 'Lexington'
         when `18741` = '1' then 'Lima'
         when `18742` = '1' then 'Lincoln & Hastings-Krny'
         when `18743` = '1' then 'Little Rock-Pine Bluff'
         when `18744` = '1' then 'Los Angeles'
         when `18745` = '1' then 'Louisville'
         when `18746` = '1' then 'Lubbock'
         when `18747` = '1' then 'Macon'
         when `18748` = '1' then 'Madison'
         when `18749` = '1' then 'Mankato'
         when `18750` = '1' then 'Marquette'
         when `18751` = '1' then 'Medford-Klamath Falls'
         when `18752` = '1' then 'Memphis'
         when `18753` = '1' then 'Meridian'
         when `18754` = '1' then 'Miami-Ft. Lauderdale'
         when `18755` = '1' then 'Milwaukee'
         when `18756` = '1' then 'Minneapolis-St. Paul'
         when `18757` = '1' then 'Minot-Bsmrck-Dcknsn(Wlstn)'
         when `18758` = '1' then 'Missoula'
         when `18759` = '1' then 'Mobile-Pensacola (Ft Walt)'
         when `18760` = '1' then 'Monroe-El Dorado'
         when `18761` = '1' then 'Monterey-Salinas'
         when `18762` = '1' then 'Montgomery-Selma'
         when `18763` = '1' then 'Myrtle Beach-Florence'
         when `18764` = '1' then 'Nashville'
         when `18765` = '1' then 'New Orleans'
         when `18766` = '1' then 'New York'
         when `18767` = '1' then 'Norfolk-Portsmth-Newpt Nws'
         when `18768` = '1' then 'North Platte'
         when `18769` = '1' then 'Odessa-Midland'
         when `18770` = '1' then 'Oklahoma City'
         when `18771` = '1' then 'Omaha'
         when `18772` = '1' then 'Orlando-Daytona Bch-Melbrn'
         when `18773` = '1' then 'Ottumwa-Kirksville'
         when `18774` = '1' then 'Paducah-Cape Girard-Harsbg'
         when `18775` = '1' then 'Palm Springs'
         when `18776` = '1' then 'Panama City'
         when `18777` = '1' then 'Parkersburg'
         when `18778` = '1' then 'Peoria-Bloomington'
         when `18779` = '1' then 'Philadelphia'
         when `18780` = '1' then 'Phoenix (Prescott)'
         when `18781` = '1' then 'Pittsburgh'
         when `18782` = '1' then 'Portland, OR'
         when `18783` = '1' then 'Portland-Auburn'
         when `18784` = '1' then 'Presque Isle'
         when `18785` = '1' then 'Providence-New Bedford'
         when `18786` = '1' then 'Quincy-Hannibal-Keokuk'
         when `18787` = '1' then 'Raleigh-Durham (Fayetvlle)'
         when `18788` = '1' then 'Rapid City'
         when `18789` = '1' then 'Reno'
         when `18790` = '1' then 'Richmond-Petersburg'
         when `18791` = '1' then 'Roanoke-Lynchburg'
         when `18792` = '1' then 'Rochester, NY'
         when `18793` = '1' then 'Rochestr-Mason City-Austin'
         when `18794` = '1' then 'Rockford'
         when `18795` = '1' then 'Sacramnto-Stkton-Modesto'
         when `18796` = '1' then 'Salisbury'
         when `18797` = '1' then 'Salt Lake City'
         when `18798` = '1' then 'San Angelo'
         when `18799` = '1' then 'San Antonio'
         when `18800` = '1' then 'San Diego'
         when `18801` = '1' then 'San Francisco-Oak-San Jose'
         when `18802` = '1' then 'Santabarbra-Sanmar-Sanluob'
         when `18803` = '1' then 'Savannah'
         when `18804` = '1' then 'Seattle-Tacoma'
         when `18805` = '1' then 'Sherman-Ada'
         when `18806` = '1' then 'Shreveport'
         when `18807` = '1' then 'Sioux City'
         when `18808` = '1' then 'Sioux Falls(Mitchell)'
         when `18809` = '1' then 'South Bend-Elkhart'
         when `18810` = '1' then 'Spokane'
         when `18811` = '1' then 'Springfield, MO'
         when `18812` = '1' then 'Springfield-Holyoke'
         when `18813` = '1' then 'St. Joseph'
         when `18814` = '1' then 'St. Louis'
         when `18815` = '1' then 'Syracuse'
         when `18816` = '1' then 'Tallahassee-Thomasville'
         when `18817` = '1' then 'Tampa-St. Pete (Sarasota)'
         when `18818` = '1' then 'Terre Haute'
         when `18819` = '1' then 'Toledo'
         when `18820` = '1' then 'Topeka'
         when `18821` = '1' then 'Traverse City-Cadillac'
         when `18822` = '1' then 'Tri-Cities, TN-VA'
         when `18823` = '1' then 'Tucson (Sierra Vista)'
         when `18824` = '1' then 'Tulsa'
         when `18825` = '1' then 'Twin Falls'
         when `18826` = '1' then 'Tyler-Longview(Lfkn&Ncgd)'
         when `18827` = '1' then 'Utica'
         when `18828` = '1' then 'Victoria'
         when `18829` = '1' then 'Waco-Temple-Bryan'
         when `18830` = '1' then 'Washington, DC (Hagrstwn)'
         when `18831` = '1' then 'Watertown'
         when `18832` = '1' then 'Wausau-Rhinelander'
         when `18833` = '1' then 'West Palm Beach-Ft. Pierce'
         when `18834` = '1' then 'Wheeling-Steubenville'
         when `18835` = '1' then 'Wichita Falls & Lawton'
         when `18836` = '1' then 'Wichita-Hutchinson Plus'
         when `18837` = '1' then 'Wilkes Barre-Scranton-Hztn'
         when `18838` = '1' then 'Wilmington'
         when `18839` = '1' then 'Yakima-Pasco-Rchlnd-Knnwck'
         when `18840` = '1' then 'Youngstown'
         when `18841` = '1' then 'Yuma-El Centro'
         when `18842` = '1' then 'Zanesville'
         else null
       end dma_description,
       case
         when `18633` = '1' then 18633
         when `18634` = '1' then 18634
         when `18635` = '1' then 18635
         when `18636` = '1' then 18636
         when `18637` = '1' then 18637
         when `18638` = '1' then 18638
         when `18639` = '1' then 18639
         when `18640` = '1' then 18640
         when `18641` = '1' then 18641
         when `18642` = '1' then 18642
         when `18643` = '1' then 18643
         when `18644` = '1' then 18644
         when `18645` = '1' then 18645
         when `18646` = '1' then 18646
         when `18647` = '1' then 18647
         when `18648` = '1' then 18648
         when `18649` = '1' then 18649
         when `18650` = '1' then 18650
         when `18651` = '1' then 18651
         when `18652` = '1' then 18652
         when `18653` = '1' then 18653
         when `18654` = '1' then 18654
         when `18655` = '1' then 18655
         when `18656` = '1' then 18656
         when `18657` = '1' then 18657
         when `18658` = '1' then 18658
         when `18659` = '1' then 18659
         when `18660` = '1' then 18660
         when `18661` = '1' then 18661
         when `18662` = '1' then 18662
         when `18663` = '1' then 18663
         when `18664` = '1' then 18664
         when `18665` = '1' then 18665
         when `18666` = '1' then 18666
         when `18667` = '1' then 18667
         when `18668` = '1' then 18668
         when `18669` = '1' then 18669
         when `18670` = '1' then 18670
         when `18671` = '1' then 18671
         when `18672` = '1' then 18672
         when `18673` = '1' then 18673
         when `18674` = '1' then 18674
         when `18675` = '1' then 18675
         when `18676` = '1' then 18676
         when `18677` = '1' then 18677
         when `18678` = '1' then 18678
         when `18679` = '1' then 18679
         when `18680` = '1' then 18680
         when `18681` = '1' then 18681
         when `18682` = '1' then 18682
         when `18683` = '1' then 18683
         when `18684` = '1' then 18684
         when `18685` = '1' then 18685
         when `18686` = '1' then 18686
         when `18687` = '1' then 18687
         when `18688` = '1' then 18688
         when `18689` = '1' then 18689
         when `18690` = '1' then 18690
         when `18691` = '1' then 18691
         when `18692` = '1' then 18692
         when `18693` = '1' then 18693
         when `18694` = '1' then 18694
         when `18695` = '1' then 18695
         when `18696` = '1' then 18696
         when `18697` = '1' then 18697
         when `18698` = '1' then 18698
         when `18699` = '1' then 18699
         when `18700` = '1' then 18700
         when `18701` = '1' then 18701
         when `18702` = '1' then 18702
         when `18703` = '1' then 18703
         when `18704` = '1' then 18704
         when `18705` = '1' then 18705
         when `18706` = '1' then 18706
         when `18707` = '1' then 18707
         when `18708` = '1' then 18708
         when `18709` = '1' then 18709
         when `18710` = '1' then 18710
         when `18711` = '1' then 18711
         when `18712` = '1' then 18712
         when `18713` = '1' then 18713
         when `18714` = '1' then 18714
         when `18715` = '1' then 18715
         when `18716` = '1' then 18716
         when `18717` = '1' then 18717
         when `18718` = '1' then 18718
         when `18719` = '1' then 18719
         when `18720` = '1' then 18720
         when `18721` = '1' then 18721
         when `18722` = '1' then 18722
         when `18723` = '1' then 18723
         when `18724` = '1' then 18724
         when `18725` = '1' then 18725
         when `18726` = '1' then 18726
         when `18727` = '1' then 18727
         when `18728` = '1' then 18728
         when `18729` = '1' then 18729
         when `18730` = '1' then 18730
         when `18731` = '1' then 18731
         when `18732` = '1' then 18732
         when `18733` = '1' then 18733
         when `18734` = '1' then 18734
         when `18735` = '1' then 18735
         when `18736` = '1' then 18736
         when `18737` = '1' then 18737
         when `18738` = '1' then 18738
         when `18739` = '1' then 18739
         when `18740` = '1' then 18740
         when `18741` = '1' then 18741
         when `18742` = '1' then 18742
         when `18743` = '1' then 18743
         when `18744` = '1' then 18744
         when `18745` = '1' then 18745
         when `18746` = '1' then 18746
         when `18747` = '1' then 18747
         when `18748` = '1' then 18748
         when `18749` = '1' then 18749
         when `18750` = '1' then 18750
         when `18751` = '1' then 18751
         when `18752` = '1' then 18752
         when `18753` = '1' then 18753
         when `18754` = '1' then 18754
         when `18755` = '1' then 18755
         when `18756` = '1' then 18756
         when `18757` = '1' then 18757
         when `18758` = '1' then 18758
         when `18759` = '1' then 18759
         when `18760` = '1' then 18760
         when `18761` = '1' then 18761
         when `18762` = '1' then 18762
         when `18763` = '1' then 18763
         when `18764` = '1' then 18764
         when `18765` = '1' then 18765
         when `18766` = '1' then 18766
         when `18767` = '1' then 18767
         when `18768` = '1' then 18768
         when `18769` = '1' then 18769
         when `18770` = '1' then 18770
         when `18771` = '1' then 18771
         when `18772` = '1' then 18772
         when `18773` = '1' then 18773
         when `18774` = '1' then 18774
         when `18775` = '1' then 18775
         when `18776` = '1' then 18776
         when `18777` = '1' then 18777
         when `18778` = '1' then 18778
         when `18779` = '1' then 18779
         when `18780` = '1' then 18780
         when `18781` = '1' then 18781
         when `18782` = '1' then 18782
         when `18783` = '1' then 18783
         when `18784` = '1' then 18784
         when `18785` = '1' then 18785
         when `18786` = '1' then 18786
         when `18787` = '1' then 18787
         when `18788` = '1' then 18788
         when `18789` = '1' then 18789
         when `18790` = '1' then 18790
         when `18791` = '1' then 18791
         when `18792` = '1' then 18792
         when `18793` = '1' then 18793
         when `18794` = '1' then 18794
         when `18795` = '1' then 18795
         when `18796` = '1' then 18796
         when `18797` = '1' then 18797
         when `18798` = '1' then 18798
         when `18799` = '1' then 18799
         when `18800` = '1' then 18800
         when `18801` = '1' then 18801
         when `18802` = '1' then 18802
         when `18803` = '1' then 18803
         when `18804` = '1' then 18804
         when `18805` = '1' then 18805
         when `18806` = '1' then 18806
         when `18807` = '1' then 18807
         when `18808` = '1' then 18808
         when `18809` = '1' then 18809
         when `18810` = '1' then 18810
         when `18811` = '1' then 18811
         when `18812` = '1' then 18812
         when `18813` = '1' then 18813
         when `18814` = '1' then 18814
         when `18815` = '1' then 18815
         when `18816` = '1' then 18816
         when `18817` = '1' then 18817
         when `18818` = '1' then 18818
         when `18819` = '1' then 18819
         when `18820` = '1' then 18820
         when `18821` = '1' then 18821
         when `18822` = '1' then 18822
         when `18823` = '1' then 18823
         when `18824` = '1' then 18824
         when `18825` = '1' then 18825
         when `18826` = '1' then 18826
         when `18827` = '1' then 18827
         when `18828` = '1' then 18828
         when `18829` = '1' then 18829
         when `18830` = '1' then 18830
         when `18831` = '1' then 18831
         when `18832` = '1' then 18832
         when `18833` = '1' then 18833
         when `18834` = '1' then 18834
         when `18835` = '1' then 18835
         when `18836` = '1' then 18836
         when `18837` = '1' then 18837
         when `18838` = '1' then 18838
         when `18839` = '1' then 18839
         when `18840` = '1' then 18840
         when `18841` = '1' then 18841
         when `18842` = '1' then 18842
         else null
       end dma_code,
       case
         when `4131` = '1' then '18 - 24'
         when `4132` = '1' then '25 - 29'
         when `4133` = '1' then '30 - 34'
         when `4134` = '1' then '35 - 39'
         when `4135` = '1' then '40 - 44'
         when `4136` = '1' then '45 - 49'
         when `4137` = '1' then '50 - 54'
         when `4138` = '1' then '55 - 59'
         when `4139` = '1' then '60 - 64'
         when `4140` = '1' then '65 +'
         else null
       end hh_age_description,
       case
         when `4131` = '1' then 1
         when `4132` = '1' then 2
         when `4133` = '1' then 3
         when `4134` = '1' then 4
         when `4135` = '1' then 5
         when `4136` = '1' then 6
         when `4137` = '1' then 7
         when `4138` = '1' then 8
         when `4139` = '1' then 9
         when `4140` = '1' then 10
         else null
       end hh_age_code,
       case
         when `800885401` = '1' then 'Less than $15,000'
         when `800885402` = '1' then '$15,000 - $19,999'
         when `800885403` = '1' then '$20,000 - $29,999'
         when `800885404` = '1' then '$30,000 - $39,999'
         when `800885405` = '1' then '$40,000 - $49,999'
         when `800885406` = '1' then '$50,000 - $59,999'
         when `800885407` = '1' then '$60,000 - $69,999'
         when `800885408` = '1' then '$70,000 - $79,999'
         when `800885409` = '1' then '$80,000 - $89,999'
         when `800885410` = '1' then '$90,000 - $99,999'
         when `800885411` = '1' then '$100,000 - $124,999'
         when `800885412` = '1' then '$125,000 - $149,999'
         when `800885413` = '1' then 'Greater than $149,999'
         else null
       end hh_income_description,
       case
         when `800885401` = '1' then 1
         when `800885402` = '1' then 2
         when `800885403` = '1' then 3
         when `800885404` = '1' then 4
         when `800885405` = '1' then 5
         when `800885406` = '1' then 6
         when `800885407` = '1' then 7
         when `800885408` = '1' then 8
         when `800885409` = '1' then 9
         when `800885410` = '1' then 10
         when `800885411` = '1' then 11
         when `800885412` = '1' then 12
         when `800885413` = '1' then 13
         else null
       end hh_income_code,
       case
         when `1199` = '1' then 'Asian'
         when `1200` = '1' then 'African American'
         when `1202` = '1' then 'White'
         when `10500` = '1' then 'Hispanic'
         else null
       end hh_race_description,
       case
         when `1199` = '1' then 1
         when `1200` = '1' then 2
         when `1202` = '1' then 3
         when `10500` = '1' then 4
         else null
       end hh_race_code,
       case
         when `1889` = '1' then '1 person'
         when `1890` = '1' then '2 people'
         when `1891` = '1' then '3 people'
         when `1892` = '1' then '4 people'
         when `1893` = '1' then '5 people'
         when `1894` = '1' then '6 people'
         when `1895` = '1' then '7 people'
         when `1896` = '1' then '8 people'
         when `1897` = '1' then '9 or more people'
         else null
       end hh_size_description,
       case
         when `1889` = '1' then 1
         when `1890` = '1' then 2
         when `1891` = '1' then 3
         when `1892` = '1' then 4
         when `1893` = '1' then 5
         when `1894` = '1' then 6
         when `1895` = '1' then 7
         when `1896` = '1' then 8
         when `1897` = '1' then 9
         else null
       end hh_size_code
from default.jwillingham_acxiom_paf tb1
  left join (select t1.zip_code,
                    t1.fips_county,
                    t2.state_abbreviation,
                    t2.county_name,
                    t2.rural_urban_county_code_2013,
                    t2.description
             from default.jwillingham_zip_county t1
               join default.jwillingham_rural_urban_codes t2 on t1.fips_county = t2.fips_county) tb2 on tb1.zip = tb2.zip_code;

-- READY DEMOGRAPHIC AND GEOGRAPHIC ATTRIBUTES
-- Step 2: Collapse attributes and obtain household_pel IDs.
-- https://api.qubole.com/v2/analyze?command_id=136452200
set hive.auto.convert.join = true;
set hive.exec.compress.intermediate = true;
set hive.execution.engine = mr;
set hive.map.aggr.hash.percentmemory = 0.125;
set hive.vectorized.execution.enabled = true;
set mapreduce.jobtracker.expire.trackers.interval = 1800000;
set mapreduce.map.java.opts = -Xmx3072m;
set mapreduce.map.memory.mb = 4096;
set mapreduce.reduce.java.opts = -Xmx6144m;
set mapreduce.reduce.memory.mb = 8192;
set mapreduce.task.timeout = 1800000;

drop table if exists default.jwillingham_household_demographics_step2;

create table default.jwillingham_household_demographics_step2 as
select distinct household_pel,
       case
         when hh_age_code in (1,2,3) then 1
         when hh_age_code in (4,5,6,7) then 2
         when hh_age_code in (8,9,10) then 3
         else 0
       end hh_age_code,
       case
         when hh_age_code in (1,2,3) then "18 - 34"
         when hh_age_code in (4,5,6,7) then "35 - 54"
         when hh_age_code in (8,9,10) then "55 +"
         else "unknown"
       end hh_age_description,
       case
         when hh_income_code in (1,2) then 1
         when hh_income_code in (3,4,5) then 2
         when hh_income_code in (6,7,8,9,10) then 3
         when hh_income_code in (11,12,13) then 4
         else 0
       end hh_income_code,
       case
         when hh_income_code in (1,2) then "< $20,000"
         when hh_income_code in (3,4,5) then "$20,000 - $49,999"
         when hh_income_code in (6,7,8,9,10) then "$50,000 - $99,999"
         when hh_income_code in (11,12,13) then "$100,000 +"
         else "unknown"
       end hh_income_description,
       case
         when hh_size_code in (1,2) then 1
         when hh_size_code in (3,4) then 2
         when hh_size_code in (5,6,7,8,9) then 3
         else 0
       end hh_size_code,
       case
         when hh_size_code in (1,2) then "1 - 2"
         when hh_size_code in (3,4) then "3 - 4"
         when hh_size_code in (5,6,7,8,9) then "5 +"
         else "unknown"
       end hh_size_description,
       case
         when hh_race_code is null then 0
         else hh_race_code
       end hh_race_code,
       case
         when hh_race_description is null then "unknown"
         else hh_race_description
       end hh_race_description,
       case
         when population_density_code = 1 then 1
         when population_density_code = 2 then 2
         when population_density_code = 3 then 3
         when population_density_code = 4 then 4
         when population_density_code = 5 then 5
         when population_density_code in (6,8) then 6
         when population_density_code in (7,9) then 7
         else 0
       end population_density_code,
       case
         when population_density_code = 1 then "Counties in metro areas of 1 million population or more"
         when population_density_code = 2 then "Counties in metro areas of 250,000 to 1 million population"
         when population_density_code = 3 then "Counties in metro areas of fewer than 250,000 population"
         when population_density_code = 4 then "Urban population of 20,000 or more, adjacent to a metro area"
         when population_density_code = 5 then "Urban population of 20,000 or more, not adjacent to a metro area"
         when population_density_code in (6,8) then "Urban population less than 20,000, adjacent to a metro area"
         when population_density_code in (7,9) then "Urban population less than 20,000, not adjacent to a metro area"
         else "unknown"
       end population_density_description,
       case
         when dma_code is null then 0
         else dma_code
       end dma_code,
       case
         when dma_description is null then "Balance of United States"
         else dma_description
       end dma_description
from default.jwillingham_household_demographics_step1 tb1
  join default.acxiom_identitylink_orc tb2 on tb1.consumer_pel = tb2.individual_link
where substr(consumer_pel,1,2) != 'Xi'
and   household_pel != ''
and   hh_age_code is not null;

-- READY DEMOGRAPHIC AND GEOGRAPHIC ATTRIBUTES
-- Step 3: Identify duplicate household_pel IDs and add random_number.
-- https://api.qubole.com/v2/analyze?command_id=136469190
drop table if exists default.jwillingham_household_demographics_step3;

create table default.jwillingham_household_demographics_step3 as
select tb1.*,
       case
         when duplicate_count > 1 then 1
         else 0
       end duplicate_flag,
       rand(21347) random_number
from default.jwillingham_household_demographics_step2 tb1
  join (select household_pel,
               count(*) duplicate_count
        from default.jwillingham_household_demographics_step2
        group by household_pel) tb2 on tb1.household_pel = tb2.household_pel;

-- READY DEMOGRAPHIC AND GEOGRAPHIC ATTRIBUTES
-- Step 4: Select household_pel IDs with the largest random_number.
-- https://api.qubole.com/v2/analyze?command_id=136475385
drop table if exists default.jwillingham_household_demographics_step4;

create table default.jwillingham_household_demographics_step4 as
select household_pel,
       max(random_number) random_number_max
from default.jwillingham_household_demographics_step3
group by household_pel;

-- READY DEMOGRAPHIC AND GEOGRAPHIC ATTRIBUTES
-- Step 5: Join to table derived in Step 3 on the largest random_number within each household_pel ID.
-- https://api.qubole.com/v2/analyze?command_id=136476687
drop table if exists default.jwillingham_household_demographics_step5;

create table default.jwillingham_household_demographics_step5 as
select tb1.*
from default.jwillingham_household_demographics_step3 tb1
  join default.jwillingham_household_demographics_step4 tb2
    on tb1.household_pel = tb2.household_pel
   and tb1.random_number = tb2.random_number_max;