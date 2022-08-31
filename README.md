# Opioid-Regulations-Evaluations

The Purpose of this analysis is to evaluate the effectiveness of the opioid regulations issued by the state of Florida, Washington and Texas on the opioid shipments and deaths cause by opioid overdose in these states. Moreover, we also look into how the effect of opioid vary by income in each state. The control states were identified as the ones that had a similar shipments per capita or deaths per capita (depending on the analysis) before the policy was implimented. Moreover, it was also ensured that no opioid regulation was implimented in these states. We used a differences-in-differences approach to evaluate the causal effect of each policy. 
The details of the analysis can be found in the report. 

The datasets have been provided in the [this github repository](https://github.com/anas14680/Opioid-Regulations-Evaluations/tree/master/00_source_data). However, while uploading the shipment data we breached the limits of git lfs as the data was 80GB in size. The source for this dataset has also been added to the report. The data can be found [here](https://www.washingtonpost.com/national/2019/07/18/how-download-use-dea-pain-pills-database/) as well.
The python sript used to processes this data has been placed in [this](https://github.com/anas14680/Opioid-Regulations-Evaluations/blob/master/10_code/cleaning_shipment_data.py) file. 
