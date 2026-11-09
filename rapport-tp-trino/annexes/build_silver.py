import re, pandas as pd, trino
u = pd.read_csv("gen/users.csv", dtype=str, keep_default_na=False)
o = pd.read_csv("gen/orders.csv", dtype=str, keep_default_na=False)
lines = u["address"].str.split("\n")
l1 = lines.str[0]; l2 = lines.str[1]
def city(s):
    m = re.match(r"^(.+), [A-Z]{2} [0-9]{5}$", s); return m.group(1) if m else None
def st(s):
    m = re.search(r"([A-Z]{2}) ([0-9]{5})$", s); return (m.group(1), m.group(2)) if m else ("","")
users = pd.DataFrame({
 "user_id":u.uuid,"username":u.username.str.strip(),"name":u.name.str.strip(),
 "sex":u.sex.str.strip().str.upper(),"email":u.mail.str.strip().str.lower(),
 "birthdate":u.birthdate,"street":l1,"city":l2.map(city),
 "state":l2.map(lambda s:st(s)[0]),"zip_code":l2.map(lambda s:st(s)[1])})
orders = pd.DataFrame({"order_id":o.uuid,"user_id":o.user_uuid,"ordered_at":o.date,
 "quantity":o.quantity.astype(int),"product":o["product"].str.strip().str.lower()}).drop_duplicates("order_id")
print(len(users), len(orders))
c = trino.dbapi.connect(host="localhost",port=8080,user="lab",catalog="lakehouse")
def run(q):
    cur=c.cursor(); cur.execute(q); return cur.fetchall()
run("CREATE SCHEMA IF NOT EXISTS lakehouse.silver")
run("""CREATE TABLE IF NOT EXISTS lakehouse.silver.users (user_id varchar, username varchar, name varchar, sex varchar, email varchar, birthdate date, street varchar, city varchar, state varchar, zip_code varchar)""")
run("""CREATE TABLE IF NOT EXISTS lakehouse.silver.orders (order_id varchar, user_id varchar, ordered_at timestamp(6) with time zone, quantity integer, product varchar)""")
q = lambda v: "NULL" if (v is None or v != v) else "'"+str(v).replace("'","''")+"'"
rows = [f"({q(r.user_id)},{q(r.username)},{q(r['name'])},{q(r.sex)},{q(r.email)},DATE {q(r.birthdate)},{q(r.street)},{q(r.city)},{q(r.state)},{q(r.zip_code)})" for _,r in users.iterrows()]
run("INSERT INTO lakehouse.silver.users VALUES "+",".join(rows))
rows = [f"({q(r.order_id)},{q(r.user_id)},TIMESTAMP {q(r.ordered_at.replace(' ','T') if False else r.ordered_at)} ,{r.quantity},{q(r['product'])})" for _,r in orders.iterrows()]
for i in range(0,len(rows),500):
    run("INSERT INTO lakehouse.silver.orders VALUES "+",".join(rows[i:i+500]))
print(run("select count(*) from lakehouse.silver.orders"), run("select count(*) from lakehouse.silver.users"))
