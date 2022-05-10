# Introduction and Motivation

The project presented is an extension of the twist challenge project [time-series-sales-analytics](https://github.com/SitwalaM/time-series-sales-analytics). The objective of the project is to implement a business intelligence pipeline for a small shop which gives the shop owner insights that can drive actionable insights. The main components of the project are;

### 1. Sales Forescasting

A sales forecasting model using Prophet is implemented to help the client (shop owner) plan for the month. The model is currently implementing a 7-day forecast as only 13 months of data is demonstrated here.

### 2. Customer Segmentation

Clustering is implemented using Recency, Frequency and Monetary value of each customer. The segmentation allows for the shop owner to target specific customers for promotions and retention calls.

### 3. Customer Growth Budget

The customer growth budget tracks the growth of customers and classifies them into the following categories;
* New: First time visitors
* Churned: Customers who haven't visited for 2 months consecutively (defined by the client)
* Regained: Previously churned customers who have visited the shop again.

Growth budget is typically used in subscription based services but it can be a powerful insight in this scenario as salons have a database of regular visitors who can be treated as subcribers for analysis purposes.

### 4. Cloud Implementation and Orchestration

The implementation is planned for deployment completely in the cloud. A MySQL server was setup on and Amazon EC2. Apache Airflow is used for the pipeling orchestration with a daily interval. Currently, the extraction of the data is not done from the client premises. For demonstration purposes, the front-end application allows for the client to upload the latest sales data. Orchestraion is demonstrated on a Desktop PC in a completely virtual environment which can be deployed on the Amazon EC2- The t2.micro EC2 used for the project demonstration does not have enough capacity to install Prophet.

# Tools and Models Used 
* Prophet - Time-Seres Forecasting
* Kmeans - Customer Segmentation
* Apache Airflow - ETL orchestration
* MySQL 
* Streamlit - front-end web app for viewing insights

# Conclusions and Future Work

The insights presented in [Notebook](https://github.com/SitwalaM/sales_business_intelligence_capstone/blob/main/overview_notebook.ipynb) can provide a business owner a basis for decisions that can drive growth. 

* The simple forecasting model implemented helps the owner plan for stock and worker's availability
* The customer segmentation shows that less frequent and low spend customers drive most of the revenue. This insight can help drive strategy on marketing. 
* The growth budget shows that rate of churn and new customers is almost stationary. Perhaps the definition of churn should be revised to more than two months.

## Future Work

* On-Premises automated extraction of the data from the source
* Migration of the apache airflow orchestration from the Desktop PC to the cloud.
* Automated action recommendation for retention calls based on the output from the segmentation. For example, high spending customers with a poor recency should be offered a discount to return to the store.

