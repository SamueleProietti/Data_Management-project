import pymongo
import time
from datetime import datetime
import random
from time import perf_counter

# Connessione a MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["grocery_sales"]

BATCH = 100000000  # batch grande per ridurre i round-trip quando svuoti il cursore

def agg(coll, pipeline):
    """
    Wrapper per avere sempre allowDiskUse=True e impostare il batch_size del cursore.
    coll.aggregate(...) restituisce un cursor; qui gli applichiamo il batch_size.
    """
    cur = coll.aggregate(pipeline, allowDiskUse=True)
    return cur.batch_size(BATCH)


# Lista delle query MongoDB (aggregate pipelines)
queries = [
    # Q1 - Top 10 categories by number of customers served per employee 1
    lambda: agg(db.sales_flat3, [
                { "$match": { "Employee.EmployeeID": 1 } },
                { "$group": {
                    "_id": "$Product.CategoryName",
                    "customers": { "$addToSet": "$Customer.CustomerID" }
                }},
                { "$project": { "_id": 0, "category": "$_id", "distinct_customers": { "$size": "$customers" } } },
                { "$sort": { "distinct_customers": -1, "category": 1 } },
                { "$limit": 10 }
            ]),

    # Q2 - Top 10 cities by number of unique employees who sold in the Meat category
    lambda: agg(db.sales_flat3, [
                { "$match": { "Product.CategoryName": "Meat" } },
                { "$group": {
                    "_id": "$Customer.CityName",
                    "employees": { "$addToSet": "$Employee.EmployeeID" }
                }},
                { "$project": {
                    "_id": 0,
                    "city": "$_id",
                    "distinct_employees": { "$size": "$employees" }
                }},
                { "$sort": { "distinct_employees": -1, "city": 1 } },
                { "$limit": 10 }
            ]),

    # Q3 - Revenue per Product in Dallas
    lambda: agg(db.sales_flat3, [
                { "$match": { "Customer.CityName": "Dallas" } },
                { "$group": {
                    "_id": "$Product.ProductName",
                    "revenue": { "$sum": { "$multiply": ["$Quantity", "$Product.Price", { "$subtract": [1, "$Discount"] }] } }
                }},
                { "$sort": { "revenue": -1 } }
            ]),
    
    # Q4 - Price Distribution in the Meat Category
    lambda: agg(db.sales_flat3, [
                { "$match": { "Product.CategoryName": "Meat" } },
                { "$bucket": {
                    "groupBy": "$Product.Price",
                    "boundaries": [0, 10, 20, 30, 40, 50, 1000],
                    "default": ">=1000",
                    "output": { "count": { "$sum": 1 } }
                }}
            ]),
    

    # Q5 - Top 10 Customers in Tucson
    lambda: agg(db.sales_flat3, [
                { "$match": { "Customer.CityName": "Tucson" } },
                { "$group": {
                    "_id": { "id": "$Customer.CustomerID", "first": "$Customer.FirstName", "last": "$Customer.LastName" },
                    "purchases": { "$sum": 1 },
                    "spend": { "$sum": { "$multiply": ["$Quantity", "$Product.Price", { "$subtract": [1, "$Discount"] }] } }
                }},
                { "$sort": { "spend": -1 } },
                { "$limit": 10 }
            ]),

    # Q6 - Least Sold Product in San Diego
    lambda: agg(db.sales_flat3, [
                { "$match": { "Customer.CityName": "San Diego" } },            
                { "$group": {
                    "_id": "$Product.ProductName",
                    "qty": { "$sum": "$Quantity" }
                }},
                { "$sort": { "qty": 1, "_id": 1 } },                               
                { "$limit": 1 },
                { "$project": { "_id": 0, "City": "San Diego", "Product": "$_id", "QuantitySold": "$qty" } }
            ]),

    # Q7 - Top 10 Customers by Number of Distinct Products Purchased
    lambda: agg(db.sales_flat3, [
                { "$group": {
                    "_id": "$Customer.CustomerID",
                    "products": { "$addToSet": "$Product.ProductID" },
                    "firstname": { "$first": "$Customer.FirstName" },
                    "lastname":  { "$first": "$Customer.LastName" }
                }},
                { "$project": {
                    "customerid": "$_id",
                    "firstname": 1,
                    "lastname": 1,
                    "distinct_products": { "$size": "$products" },
                    "_id": 0
                }},
                { "$sort": { "distinct_products": -1 } },
                { "$limit": 10 }
            ]),

    # Q8 - Top 10 employees by total revenue in the Beverages category
    lambda: agg(db.sales_flat3, [
                { "$match": { "Product.CategoryName": "Beverages" } },
                { "$group": {
                    "_id": { "id": "$Employee.EmployeeID", "first": "$Employee.FirstName", "last": "$Employee.LastName" },
                    "revenue": { "$sum": { "$multiply": [ "$Quantity", "$Product.Price", { "$subtract": [1, "$Discount"] } ] } }
                }},
                { "$project": {
                    "_id": 0,
                    "EmployeeID": "$_id.id",
                    "FirstName": "$_id.first",
                    "LastName": "$_id.last",
                    "revenue": { "$round": ["$revenue", 2] }
                }},
                { "$sort": { "revenue": -1, "EmployeeID": 1 } },
                { "$limit": 10 }
            ]),

    # Q9 - Top 10 employees by distinct products sold in Cereals category
    lambda: agg(db.sales_flat3, [
                { "$match": { "Product.CategoryName": "Cereals" } },
                { "$group": {
                    "_id": { "id": "$Employee.EmployeeID", "first": "$Employee.FirstName", "last": "$Employee.LastName" },
                    "products": { "$addToSet": "$Product.ProductID" }
                }},
                { "$project": {
                    "_id": 0,
                    "EmployeeID": "$_id.id",
                    "FirstName": "$_id.first",
                    "LastName": "$_id.last",
                    "distinct_products": { "$size": "$products" }
                }},
                { "$sort": { "distinct_products": -1, "EmployeeID": 1 } },
                { "$limit": 10 }
            ]),

    #Q10 - Total customer 1 spending by category (revenue)
    lambda: agg(db.sales_flat3, [
                { "$match": { "Customer.CustomerID": 1 } },
                { "$group": {
                    "_id": "$Product.CategoryName",
                    "revenue": { "$sum": { "$multiply": [ "$Quantity", "$Product.Price", { "$subtract": [1, "$Discount"] } ] } }
                }},
                { "$project": { "_id": 0, "category": "$_id", "revenue": { "$round": ["$revenue", 2] } } },
                { "$sort": { "revenue": -1, "category": 1 } }
            ]),

    #Q11 - Top 5 products purchased by the customer 1(by quantity)
    lambda: agg(db.sales_flat3, [
                { "$match": { "Customer.CustomerID": 1 } },
                { "$group": { "_id": "$Product.ProductName", "qty": { "$sum": "$Quantity" } } },
                { "$sort": { "qty": -1, "_id": 1 } },
                { "$limit": 5 },
                { "$project": { "_id": 0, "product": "$_id", "quantity": "$qty" } }
            ]),

    #Q12 - Number of sales of Scampi Tail for each city
    lambda: agg(db.sales_flat3, [
                { "$match": { "Product.ProductName": "Scampi Tail" } },
                { "$group": { "_id": "$Customer.CityName", "total": { "$sum": 1 } } },
                { "$project": { "city": "$_id", "total": 1, "_id": 0 } }
            ]),

    #Q13 - Total quantity of products belonging to a category sold in each city
    lambda: agg(db.sales_flat3, [
                { "$group": {
                    "_id": { "CategoryName": "$Product.CategoryName", "CityName": "$Customer.CityName" },
                    "total": { "$sum": "$Quantity" }
                }},
                { "$project": { "_id": 0, "city": "$_id.CityName", "category": "$_id.CategoryName", "total": 1 } },
                { "$sort": { "city": 1, "category": 1 } }
            ]),
    
    #Q14 - Number of sales of products that cost less than 30.5$ for Baltimore
    lambda: agg(db.sales_flat3, [ {"$match": {
                "$and":[{"Customer.CityName":"Baltimore"},{"Product.Price": { "$lte": 30.5 }}]
                }},
                
                {
                    "$group": {
                    "_id": "$Product.ProductName",
                    "count": { "$sum": 1 }
                    }
                },
                {
                    "$project": {
                    "res": "$_id",
                    "count": 1,
                    "_id": 0
                    }
                }
            ]),
    
    #Q15 - Revenue made by employees for selling Kiwi or Beef
    lambda: agg(db.sales_flat3, [ {"$match": {
                "$or":[{"Product.ProductName":"Beef Wellington"},{"Product.ProductName":"Kiwi"}]
                }},
                { "$group": {
                    "_id": {
                            "firstname": "$Employee.FirstName",
                        "lastname": "$Employee.LastName"
                    },
                        "total": {
                        "$sum": {
                        "$subtract": [
                            { "$multiply": [ "$Quantity", "$Product.Price" ] },
                            { "$multiply": [ "$Quantity", "$Product.Price", "$Discount" ] }
                        ]
                        }
                    }
                    }
                    }
                ,
                {
                    "$project": {
                    
                    "firstname": "$_id.firstname",
                    "lastname": "$_id.lastname",
                    "total": { "$ceil": "$total" },  
                    "_id": 0
                    }
                },
                {"$sort":{ "total":-1}}
            ]),
    
    #Q16 - Customer and date when he/she bought more than 19 units of a Produce product
    lambda: agg(db.sales_flat3, [
                {
                "$match": {                                                               
                    "$and":[{"Product.CategoryName": "Produce"},{"Quantity": {"$gte":20}}]
                }
                },
                
                {
                "$group": {
                    "_id": {
                    "CustomerName": "$Customer.FirstName",
                        "CustomerLastName":"$Customer.LastName",
                        "ProductName":"$Product.ProductName",
                        "Date":"$SalesDate"
                    }    
                }
                },
                { 
                "$project": {
                    "Name": "$_id.CustomerName",
                    "Surname": "$_id.CustomerLastName",
                    "product" :"$_id.ProductName" ,
                    "Date": "$_id.Date",
                    "_id": 0
                    }
                }
            ]),
    
    #Q17 - Top 5 products (by number of sales) in Buffalo
    lambda: agg(db.sales_flat3, [{
                "$match": {"Customer.CityName":"Buffalo" } },
                {
                "$group": { "_id": "$Product.ProductName", "total": { "$sum": 1 } 
                } 
                },
                {
                "$project": {"prod": "$_id", "total": 1, "_id": 0 } },
                {  
                    "$sort":{
                    "total":-1
                }
                },
                {
                    "$limit":5}
            ]),
    
    #Q18 - Number of sales of each Dairy product for each city
    lambda: agg(db.sales_flat3, [
                { "$match": { "Product.CategoryName": "Dairy" } },
                { "$group": { "_id": { "CityName": "$Customer.CityName", "ProductName": "$Product.ProductName" }, "total": { "$sum": 1 } } },
                { "$project": { "_id": 0, "city": "$_id.CityName", "product": "$_id.ProductName", "total": 1 } },
                { "$sort": { "city": 1, "product": 1 } }
            ]),
    
    #Q19 - Number of times an employee in Lincon has served each customer
    lambda: agg(db.sales_flat3, [
                { "$match": { "Customer.CityName": "Lincoln" } },
                { "$group": { "_id":{"EName":"$Employee.FirstName","ELastName":"$Employee.LastName",
                                "CName":"$Customer.FirstName","CLastName":"$Customer.LastName"}, "count": { "$sum": 1 } } },
                { "$project": {"EName": "$_id.EName","ELastName": "$_id.ELastName",
                            "CName": "$_id.CName","CLastName": "$_id.CLastName","count": 1, "_id": 0 } }
            ]),
    
    #Q20 - Years of employment of each employee 
    lambda: agg(db.sales_flat3, [
                { "$group": {
                    "_id": { "Name": "$Employee.FirstName", "LastName": "$Employee.LastName" },
                    "year": { "$first": "$Employee.HireDate" }
                }},
                { "$project": {
                    "_id": 0,
                    "name": "$_id.Name",
                    "lastname": "$_id.LastName",
                    "years": {
                        "$dateDiff": {
                            "startDate": { "$toDate": "$year" },
                            "endDate": datetime(2018, 1, 1),
                            "unit": "year"
                        }
                    }
                }}
            ]),
    
    #Q21 - Category and total units sold for each product
    lambda: agg(db.sales, [
                { "$group": { "_id":{"Category":"$Product.CategoryName","ProductName":"$Product.ProductName"}, "total": { "$sum": "$Quantity"} } },
                { "$project": { "category": "$_id.Category",
                                "product":"$_id.ProductName" ,
                                "total": 1,
                                "_id": 0 } },
                {"$sort":{"total": -1}} 
            ]),
    
    #Q22 - Top 10 products by number of unique customers in New York
    lambda: agg(db.sales_flat3, [
                { "$match": { "Customer.CityName": "New York" } },
                { "$group": {
                    "_id": "$Product.ProductName",
                    "customers": { "$addToSet": "$Customer.CustomerID" }
                }},
                { "$project": { "_id": 0, "product": "$_id", "distinct_customers": { "$size": "$customers" } } },
                { "$sort": { "distinct_customers": -1, "product": 1 } },
                { "$limit": 10 }
            ])
]

# Esecuzione e misurazione del tempo
# Benchmark con warm-up e due metriche:
# - TTFB (Time To First Batch): confrontabile con Compass
# - TTD  (Time To Drain): tempo per svuotare tutto il cursore (quello che misuravi prima)

RUNS = 5          # numero di misure
WARMUPS = 1       # run di riscaldamento (non conteggiate)
DRAIN = True      # misura anche lo svuotamento completo

# (opzionale) nomi carini per le query
named_queries = [(f"Q{i+1}", q) for i, q in enumerate(queries)]
#random.shuffle(named_queries)  # evita bias di cache dovuti all'ordine fisso

for name, make_cursor in named_queries:
    # warm-up
    for _ in range(WARMUPS):
        cur = make_cursor()
        try:
            next(cur)        # avanza il primo batch (allineato a Compass)
            if DRAIN:
                for _ in cur:
                    pass     # svuota il resto per scaldare bene cache e indici
        except StopIteration:
            pass

    ttfb_sum, ttd_sum = 0.0, 0.0
    for _ in range(RUNS):
        t0 = perf_counter()
        cur = make_cursor()

        # Time To First Batch
        try:
            next(cur)
        except StopIteration:
            pass
        ttfb = perf_counter() - t0
        ttfb_sum += ttfb

        # Time To Drain
        if DRAIN:
            for _ in cur:
                pass
        ttd = perf_counter() - t0
        ttd_sum += ttd
    print(f"{name} | TTFB avg: {ttfb_sum/RUNS:.4f}s | TTD avg: {ttd_sum/RUNS:.4f}s")

