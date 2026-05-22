# Platforms
Mexos = "Mexos"
Cellxpert = "Cellxpert"
NetRefer = "NetRefer"
MyAffiliates = "MyAffiliates"
Inhouse = "Inhouse"
Buffalo_Partners = "Buffalo Partners"
Smartico = "Smartico"
Q = "Q"
Income_Access = "Income Access"
SoftSwiss = "SoftSwiss"
Alanbase = "Alanbase"
EGO = "EGO"
Google_Analytics = "Google Analytics"
BRC = "BRC"
BRT= "BRT"
Voluum = "Voluum"
Spree = "Spree_gbq_sync_job"
Sweep = "Sweep"
Redtrack = "Redtrack"
Apilayer = "Apilayer"
Referon = "ReferON"
Maxmind = "Maxmind"
Sapphirebet = "Sapphirebet"
Voluum_disabled_streams = ["affiliate_network_report", "flow_report", "lander_report", "offer_report"]
BRC_incremental_streams = ["postback_3rd_party_click_log", "postback_tracking"]
BRT_incremental_streams = ["offers"]
Voluum_incremental_streams = ["affiliate_network_report", "campaign_report", "conversions", "flow_report", 
                              "lander_report", "offer_report", "traffic_source_report"]

Invalid = "Failed"

Recovery_dag_ids = ["ReprocessBuffaloPartnersExecuteAllOperatorAccounts","ReprocessCellxpertExecuteAllOperatorAccounts",
                    "ReprocessEgoExecuteAllOperatorAccounts","ReprocessGoogleAnalyticsExecuteAllOperatorAccounts",
                    "ReprocessIncomeAccessExecuteAllOperatorAccounts","ReprocessMexosExecuteAllOperatorAccounts",
                    "ReprocessMyAffiliatesExecuteAllOperatorAccounts","ReprocessNetreferExecuteAllOperatorAccounts",
                    "ReprocessQExecuteAllOperatorAccounts","ReprocessSmarticoExecuteAllOperatorAccounts",
                    "ReprocessSoftswissExecuteAllOperatorAccounts","ReprocessVoluumExecuteAllOperatorAccounts",
                    "ReprocessSweepExecuteAllOperatorAccounts","ReprocessAlanbaseExecuteAllOperatorAccounts"
                    ,"ReprocessReferonExecuteAllOperatorAccounts"]

Primary_dag_ids = ["BrtExecuteAllOperatorAccounts","BrcExecuteAllOperatorAccounts","BuffaloPartnersExecuteAllOperatorAccounts",
                   "CellxpertExecuteAllOperatorAccounts","EgoExecuteAllOperatorAccounts",
                   "GoogleAnalyticsExecuteAllOperatorAccounts","IncomeaccessExecuteAllOperatorAccounts",
                   "MexosExecuteAllOperatorAccounts","MyAffiliatesExecuteAllOperatorAccounts",
                   "NetreferExecuteAllOperatorAccounts","QExecuteAllOperatorAccounts",
                   "SmarticoExecuteAllOperatorAccounts","SoftSwissExecuteAllOperatorAccounts","AlanaseExecuteAllOperatorAccounts"
                   "VoluumExecuteAllOperatorAccounts","SweepExecuteAllOperatorAccounts","SnowflakeOAReferenceData",
                   "RedtrackExecuteAllOperatorAccounts","ApilayerExecuteAllOperatorAccounts","ReferonExecuteAllOperatorAccounts",
                   "AddModifyDeleteAirbyteSourcesAndConnections", "Api_six_sync","Api_six_update_rate_limit"]

Validate_task_names=["ValidateAll.Validate_Mexos","ValidateAll.Validate_Cellxpert",
                     "ValidateAll.Validate_Smartico","ValidateAll.Validate_SoftSwiss","ValidateAll.Validate_Alanbase"
                    "ValidateAll.Validate_NetRefer","ValidateAll.Validate_MyAffiliates",
                    "ValidateAll.Validate_Income_Access","ValidateAll.Validate_EGO",
                    "ValidateAll.Validate_Buffalo_Partners","ValidateAll.Validate_Q",
                    "ValidateAll.Validate_Sweep","ValidateAll.Validate_Redtrack", "ValidateAll.Validate_Referon"]