import psycopg2
import time
import sys
import uuid, random
from time import perf_counter

try:
    conn = psycopg2.connect(
        dbname="grocery_sales",
        user="postgres",
        password="261101", 
        host="localhost",
        port="5432"
    )
    conn.autocommit = False  # i named cursor vivono dentro la transazione
except psycopg2.Error as e:
    print(f"Error connecting to PostgreSQL Platform: {e}")
    sys.exit(1)

cur = conn.cursor()

total_time = 0
arr_string=[
    # Q1 - Top 10 categories by number of customers served per employee 1
    """SELECT
        ca.categoryname AS category,
        COUNT(DISTINCT s.customerid) AS distinct_customers
        FROM sales s
        JOIN products   p  ON p.productid   = s.productid
        JOIN categories ca ON ca.categoryid = p.categoryid
        WHERE s.salespersonid = 1
        GROUP BY ca.categoryname
        ORDER BY distinct_customers DESC, category ASC
        LIMIT 10;
        """,

    # Q2 - Top 10 cities by number of unique employees who sold in the Meat category
    """SELECT
        ci.cityname AS city,
        COUNT(DISTINCT s.salespersonid) AS distinct_employees
        FROM sales s
        JOIN products   p  ON p.productid   = s.productid
        JOIN categories ca ON ca.categoryid = p.categoryid
        JOIN customers  cu ON cu.customerid = s.customerid
        JOIN cities     ci ON ci.cityid     = cu.cityid
        WHERE ca.categoryname = 'Meat'
        GROUP BY ci.cityname
        ORDER BY distinct_employees DESC, city ASC
        LIMIT 10;
        """, 
    # Q3 - Revenue per Product in Dallas
    """SELECT p.productname AS product, ROUND(SUM(s.quantity * p.price * (1 - s.discount))::numeric, 2) AS revenue
        FROM sales s
        JOIN customers cu ON cu.customerid = s.customerid
        JOIN cities ci     ON ci.cityid     = cu.cityid
        JOIN products p    ON p.productid   = s.productid
        WHERE ci.cityname = 'Dallas'
        GROUP BY p.productname
        ORDER BY revenue DESC;
        """,
    # Q4 - Price Distribution in the Meat Category
    """SELECT bucket, COUNT(*) AS count
        FROM (
        SELECT CASE
            WHEN p.price >= 0  AND p.price < 10   THEN '0-10'
            WHEN p.price >= 10 AND p.price < 20   THEN '10-20'
            WHEN p.price >= 20 AND p.price < 30   THEN '20-30'
            WHEN p.price >= 30 AND p.price < 40   THEN '30-40'
            WHEN p.price >= 40 AND p.price < 50   THEN '40-50'
            WHEN p.price >= 50 AND p.price < 1000 THEN '50-1000'
            ELSE '>=1000'
        END AS bucket
        FROM sales s
        JOIN products   p  ON p.productid   = s.productid
        JOIN categories ca ON ca.categoryid = p.categoryid
        WHERE ca.categoryname = 'Meat'
        ) t
        GROUP BY bucket
        ORDER BY CASE bucket
        WHEN '0-10' THEN 1 WHEN '10-20' THEN 2 WHEN '20-30' THEN 3
        WHEN '30-40' THEN 4 WHEN '40-50' THEN 5 WHEN '50-1000' THEN 6
        ELSE 7 END;""",
    # Q5 - Top 10 Customers in Tucson
    """SELECT
        cu.customerid AS customerid,
        cu.firstname  AS firstname,
        cu.lastname   AS lastname,
        COUNT(*)      AS purchases,
        ROUND(SUM(s.quantity * p.price * (1 - s.discount))::numeric, 2) AS spend
        FROM sales s
        JOIN customers cu ON cu.customerid = s.customerid
        JOIN cities ci     ON ci.cityid     = cu.cityid
        JOIN products p    ON p.productid   = s.productid
        WHERE ci.cityname = 'Tucson'
        GROUP BY cu.customerid, cu.firstname, cu.lastname
        ORDER BY spend DESC
        LIMIT 10;""",
    # Q6 - Least Sold Product in San Diego
    """SELECT
        'San Diego' AS city,
        p.productname AS product,
        SUM(s.quantity) AS quantitysold
        FROM sales s
        JOIN customers cu ON cu.customerid = s.customerid
        JOIN cities ci     ON ci.cityid     = cu.cityid
        JOIN products p    ON p.productid   = s.productid
        WHERE ci.cityname = 'San Diego'
        GROUP BY p.productname
        ORDER BY quantitysold ASC, product ASC
        LIMIT 1;""",
    # Q7 - Top 10 Customers by Number of Distinct Products Purchased
    """SELECT cu.customerid, cu.firstname, cu.lastname,
            COUNT(DISTINCT s.productid) AS distinct_products
        FROM customers cu
        JOIN sales s ON s.customerid = cu.customerid
        GROUP BY cu.customerid, cu.firstname, cu.lastname
        ORDER BY distinct_products DESC
        LIMIT 10;""",
    # Q8 - Top 10 employees by total revenue in the Beverages category
    """SELECT
        e.employeeid,
        e.firstname,
        e.lastname,
        ROUND(SUM(s.quantity * p.price * (1 - s.discount))::numeric, 2) AS revenue
        FROM sales s
        JOIN products   p  ON p.productid   = s.productid
        JOIN categories ca ON ca.categoryid = p.categoryid
        JOIN employees  e  ON e.employeeid  = s.salespersonid
        WHERE ca.categoryname = 'Beverages'
        GROUP BY e.employeeid, e.firstname, e.lastname
        ORDER BY revenue DESC, e.employeeid
        LIMIT 10;
        """,
    # Q9 - Top 10 employees by distinct products sold in Cereals category
    """SELECT e.employeeid, e.firstname, e.lastname,
            COUNT(DISTINCT s.productid) AS distinct_products
        FROM sales s
        JOIN products   p  ON p.productid   = s.productid
        JOIN categories ca ON ca.categoryid = p.categoryid
        JOIN employees  e  ON e.employeeid  = s.salespersonid
        WHERE ca.categoryname = 'Cereals'
        GROUP BY e.employeeid, e.firstname, e.lastname
        ORDER BY distinct_products DESC, e.employeeid
        LIMIT 10;""",
    # Q10 - Total customer 1 spending by category (revenue)
    """SELECT ca.categoryname AS category, ROUND(SUM(s.quantity * p.price * (1 - s.discount))::numeric, 2) AS revenue
        FROM sales s
        JOIN products   p  ON p.productid   = s.productid
        JOIN categories ca ON ca.categoryid = p.categoryid
        WHERE s.customerid = 1
        GROUP BY ca.categoryname
        ORDER BY revenue DESC, category ASC;
        """,
    # Q11 - Top 5 products purchased by the customer 1(by quantity)
    """SELECT p.productname AS product, SUM(s.quantity) AS quantity
        FROM sales s
        JOIN products p ON p.productid = s.productid
        WHERE s.customerid = 1
        GROUP BY p.productname
        ORDER BY quantity DESC, product ASC
        LIMIT 5;
        """,
    # Q12 - Number of sales of Scampi Tail for each city
    """select ci.cityname, count(*)
        from products p join sales s on s.productid=p.productid join customers c on s.customerid=c.customerid join cities ci on c.cityid=ci.cityid  
        where p.productname='Scampi Tail'
        group by ci.cityname""",
    # Q13 - Total quantity of products belonging to a category sold in each city
    """select distinct ci.cityname, ca.categoryname , sum(s.quantity) unitssold
        from categories ca join products p on ca.categoryid=p.categoryid join sales s on s.productid=p.productid join customers c on c.customerid=s.customerid
        join cities ci on c.cityid=ci.cityid
        group by ca.categoryname ,ci.cityname
        order by ci.cityname""",
    # Q14 - Number of sales of products that cost less than 30.5$ for Baltimore
    """select p.productname ,count(*)   
        from sales s join products p on s.productid=p.productid join customers cu on cu.customerid=s.customerid
        join cities c on c.cityid=cu.cityid
        where p.price<=30.5 and c.cityname='Baltimore'
        group by   p.productname""",
    # Q15 - Revenue made by employees for selling Kiwi or Beef
    """select  e.firstname,e.lastname ,sum(s.quantity*p.price -s.quantity*p.price*s.discount )  total
        from cities c join employees e on c.cityid=e.cityid 
        join sales s on e.employeeid=s.salespersonid 
        join products p on p.productid=s.productid
        where p.productname='Beef Wellington' or p.productname='Kiwi'
        group by   e.firstname,e.lastname   
        order by total desc """,
    # Q16 - Customer and date when he/she bought more than 19 units of a Produce product
    """select  cu.firstname,cu.lastname, p.productname, s.salesdate      
        from customers cu join sales s on cu.customerid=s.customerid
        join products p on s.productid=p.productid join categories ca on ca.categoryid=p.categoryid 
        where s.quantity>=20 and ca.categoryname='Produce'""",
    # Q17 - Top 5 products (by number of sales) in Buffalo
    """select p.productname ,count (s.salesid)
        from sales s join customers cu on s.customerid=cu.customerid 
        join cities c on c.cityid=cu.cityid
        join products p on p.productid=s.productid
        where c.cityname='Buffalo'
        group by p.productname 
        order by count (s.salesid) desc
        limit 5  """,
    # Q18 - Number of sales of each Dairy product for each city
    """select c.cityname,p.productname,count(s.salesid)
        from categories ca join products p on p.categoryid=ca.categoryid
        join sales s on p.productid=s.productid
        join customers cu on cu.customerid=s.customerid
        join cities c on c.cityid=cu.cityid 
        where ca.categoryname='Dairy' 
        group by c.cityname,p.productname,ca.categoryname
        order by count(s.salesid) desc""",
    # Q19 - Number of times an employee in Lincoln has served each customer
    """select e.firstname,e.lastname,cu.firstname,cu.lastname,count(s.salesid)
        from customers cu join sales s on s.customerid=cu.customerid join cities c on c.cityid=cu.cityid              
        join employees e on e.employeeid=s.salespersonid
        where c.cityname='Lincoln'
        group by e.firstname,e.lastname,cu.firstname,cu.lastname""",
    # Q20 - Years of employment of each employee
    """select distinct e1.lastname,e1.firstname,2025-date_part('year', e1.hiredate) as years         
        from employees e1 join cities c on c.cityid=e1.cityid  
        group by e1.firstname,e1.lastname, years""",
    # Q21 - Category and total units sold for each product
    """select distinct ca.categoryname ,p.productname,sum(s.quantity)                                     
        from categories ca join products p on p.categoryid=ca.categoryid
        join  sales s on p.productid=s.productid 
        join customers cu on s.customerid=cu.customerid
        join cities c on c.cityid=cu.cityid
        group by ca.categoryname ,p.productname
        order by sum(s.quantity)""",
    # Q22 - Top 10 products by number of unique customers in New York
    """SELECT
        p.productname AS product,
        COUNT(DISTINCT s.customerid) AS distinct_customers
        FROM sales s
        JOIN customers cu ON cu.customerid = s.customerid
        JOIN cities ci     ON ci.cityid     = cu.cityid
        JOIN countries co  ON co.countryid  = ci.countryid
        JOIN products p    ON p.productid   = s.productid
        WHERE ci.cityname = 'New York'
        GROUP BY p.productname
        ORDER BY distinct_customers DESC, product ASC
        LIMIT 10;"""
]

def pg_stream(query: str, itersize: int = 100000000):
    """
    Crea un named cursor (server-side) per streammare i risultati.
    itersize = numero di righe per round-trip server→client.
    """
    c = conn.cursor(name=f"bench_{uuid.uuid4().hex}")
    c.itersize = itersize
    c.execute(query)
    return c

RUNS = 5
WARMUPS = 1
DRAIN = True

# Dai un nome alle query e randomizza l'ordine (riduce bias di cache)
named_queries = [(f"Q{i+1}", q) for i, q in enumerate(arr_string)]
#random.shuffle(named_queries)

for name, q in named_queries:
    # Warm-up (non cronometrato)
    for _ in range(WARMUPS):
        cur = pg_stream(q)
        cur.fetchone()  # TTFB warm-up
        if DRAIN:
            for _ in cur:  # svuota tutto
                pass
        cur.close()
        conn.rollback()   # chiude la transazione -> chiude il named cursor lato server

    ttfb_sum = 0.0
    ttd_sum  = 0.0

    for _ in range(RUNS):
        t0 = perf_counter()
        cur = pg_stream(q)

        # TTFB: tempo fino alla PRIMA riga (o None se non ci sono righe)
        _ = cur.fetchone()
        ttfb_sum += (perf_counter() - t0)

        # TTD: tempo totale fino a svuotare il cursore
        if DRAIN:
            for _ in cur:
                pass
        ttd_sum += (perf_counter() - t0)

        cur.close()
        conn.rollback()  # termina il named cursor senza effetti collaterali

    print(f"{name} | TTFB avg: {ttfb_sum/RUNS:.4f}s | TTD avg: {ttd_sum/RUNS:.4f}s")

conn.close()
