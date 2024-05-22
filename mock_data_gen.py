import random
import csv
from datetime import datetime,timedelta
import os

def generate_food_data(num_records):
    food_data = []
    for _ in range(num_records):
        order_id = random.randint(100000,9999999)
        customer_id = random.randint(100,999)
        restaurant_id = random.randint(1,99)
        order_time = (datetime.now() - timedelta(days=random.randint(1,30),hours=random.randint(1,23),minutes= random.randint(1,59))).strftime('%Y-%m-%d %H:%M:%S')
        delivery_time = (datetime.strptime(order_time, '%Y-%m-%d %H:%M:%S')+ timedelta(minutes=random.randint(10,120))).strftime('%Y-%m-%d %H:%M:%S')
        customer_location = f"Address {random.randint(1,100)}"
        restaurant_location = f"Restaurant {restaurant_id}"
        order_value = round(random.uniform(5.0,50.0),2)
        rating = random.randint(1,5)
        food_data.append([order_id,customer_id,restaurant_id,order_time,delivery_time,customer_location,restaurant_location,order_value,rating])
    return food_data
    
def write_to_csv(data,filename):
    with open(filename,mode='w',newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['order_id','customer_id','restaurant_id','order_time','delivery_time','customer_location','restaurant_location','order_value','rating'])
        writer.writerows(data)

def generate_daily_food_data(num_records,start_date):
    food_delivery_data = generate_food_data(num_records)
    folder_name = 'food_data'
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
    filename = os.path.join(folder_name,f"food_delivery_{start_date.strftime('%Y-%m-%d')}.csv")
    write_to_csv(food_delivery_data,filename)

if __name__ =="__main__":
    num_records= 10000
    start_date= datetime(2024,4,1)
    end_date = datetime(2024,4,2)

    while start_date <=end_date:
        generate_daily_food_data(num_records,start_date)
        start_date +=timedelta(days=1)

