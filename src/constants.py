COUNTER_NAMES = [
    "RRU.PrbUsedDl", 
    "DRB.UEThpDl",
    "targetTput",
    "x", "y", 
    "RF.serving.RSRP", 
    "RF.serving.RSRQ", 
    "RF.serving.RSSINR", 
    "rsrp_nb0", 
    "rsrq_nb0", 
    "rssinr_nb0", 
    "rsrp_nb1", 
    "rsrq_nb1", 
    "rssinr_nb1", 
    "rsrp_nb2", 
    "rsrq_nb2", 
    "rssinr_nb2", 
    "rsrp_nb3", 
    "rsrq_nb3", 
    "rssinr_nb3", 
    "rsrp_nb4", 
    "rsrq_nb4", 
    "rssinr_nb4"
]


UNUSED_FEATURES = ['measTimeStampRf', 
                    "nrCellIdentity", 
                    "nbCellIdentity_0", 
                    "nbCellIdentity_1", 
                    "nbCellIdentity_2", 
                    "nbCellIdentity_3", 
                    "nbCellIdentity_4",
                    "Viavi.UE.anomalies",
                    "ue-id"]

SERVICE_NAME = "pmhistory-consumer-python"

BATCH_SIZE = 32