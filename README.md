# Data Management Project

This project was developed as part of the Data Management course (University "La Sapienza", Master in Engineering in Computer Science and Artificial Intelligence, A.Y. 2025/2026) conducted by Professor Lenzerini and Professor Delfino. 

>Student: Samuele Proietti, 1946329
>
>Student: Simone Parrella, 1899329

## Project Scope

The primary focus of this project was to measure and compare the performance of two distinct database management systems: a relational database management system (**PostgreSQL**) and a document database management system (**MongoDB**). To achieve this, we selected and executed a set of 22 queries to evaluate their execution times on both database systems.

## Project Components

### Data Selection and Preparation:

We chose a comprehensive and structured Grocery Sales Database sourced from Kaggle, available through this [link]([https://www.kaggle.com/datasets/davidcariboo/player-scores](https://www.kaggle.com/datasets/andrexibiza/grocery-sales-dataset/data?select=sales.csv)), The dataset encompasses detailed information on sales transactions, customers, employees, products, categories, cities, and countries, providing a complete view of a retail sales environment.
The data was cleaned, organized, and used for our experiments to evaluate the performance of MongoDB and PostgreSQL across different query scenarios.

### Database Systems:

- **PostgreSQL**: Selected as our relational database management system.
- **MongoDB**: Chosen as our document database management system.

### Query Execution and Performance Evaluation:

We designed Python scripts to execute the selected queries on both PostgreSQL and MongoDB and report two metrics: TTFB (time to first batch/row), TTD (time to fully drain results). Each query is executed with a short warm-up pass, then repeated and averaged. The performance of both database systems was evaluated based on query execution times. All measurements are stored for analysis, and the comparative results are summarized in the project slides.

## Project Presentation

The project presentation slides in PDF format can be accessed through the following link:

- [Project Presentation Slides (PDF)](https://github.com/SamueleProietti/Data_Management-project/blob/main/MongoDB-vs-PostgreSQL.pdf)

