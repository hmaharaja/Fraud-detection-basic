# Fraud-detection-basic-AI

Created for Manulife's Winter 2021 Co-op hackathon. This model retreives masked transaction data from an Azure SQL DB and bootstraps the data to create a better balance (0.1% fraud samples -> 10% fraud samples). 
The Azure ML model was an ANN trained on 90% of the data and using a 5-fold cross-validation method, and the result of the transaction would be pushed through eventhub and displayed on a salesforce UI.
