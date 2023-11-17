from flask import Flask, jsonify, request
from faker import Faker
import random
from apscheduler.schedulers.background import BackgroundScheduler
import pandas as pd
from geopy.geocoders import GoogleV3
import os
from dotenv import load_dotenv
load_dotenv()


google_api_key = os.getenv("google_api_key")

app = Flask(__name__)
fake = Faker()


# Define the number of users, taxis, and drivers
num_users = 100
num_taxis = 30
num_drivers = 30
num_locations = 500

# Create a list of user IDs, taxi IDs, and driver IDs
user_ids = [f"User_{i}" for i in range(1, num_users + 1)]
taxi_ids = [f"Taxi_{i}" for i in range(1, num_taxis + 1)]
driver_ids = [f"Driver_{i}" for i in range(1, num_drivers + 1)]
location_ids = [i for i in range(1, num_locations + 1)]


fake_taxi_data_list = []

def generate_random_coordinates():
    # Define latitude and longitude boundaries for Milan
    min_lat, max_lat = 45.43344, 45.495083  # Approximate latitude boundaries
    min_lon, max_lon = 9.145565, 9.18951  # Approximate longitude boundaries

    # Generate random latitude and longitude within the boundaries
    latitude = round(random.uniform(min_lat, max_lat), 8)  # 6 decimal places
    longitude = round(random.uniform(min_lon, max_lon), 8)  # 6 decimal places

    return latitude, longitude

def get_address(current_location,api_key):
    latitude,longitude = current_location
    geolocator = GoogleV3(api_key=api_key)
    location = geolocator.reverse((latitude, longitude), language="en")
    return location.address




# Generate a single fake Airbnb data entry
def generate_fake_taxi_data():
    user = random.choice(user_ids)
    taxi = random.choice(taxi_ids)
    driver = random.choice(driver_ids)
    

    passenger_current_location = generate_random_coordinates()
    passenger_current_lat, passenger_current_lon = passenger_current_location

    passenger_arrival_location = generate_random_coordinates()
    passenger_arrival_lat, passenger_arrival_lon = passenger_arrival_location

    drivers_current_location = generate_random_coordinates()
    drivers_current_lat, drivers_current_lon = drivers_current_location


    drivers_location = random.choice(location_ids)
    pickup_location = random.choice(location_ids)
    destination_location = random.choice(location_ids)

    drivers_review = random.uniform(1.0, 5.0)  # Lower driver reviews
    booking_timestamp = pd.Timestamp(
        f"2023-{random.randint(1, 12)}-{random.randint(1, 28)} {random.randint(0, 23)}:{random.randint(0, 59)}"
    )
    booking_source = random.choice(["website", "app"])
    n_passengers = random.randint(1, 4)
    estimated_fare = random.uniform(10, 50)
    estimated_arrival_time = random.randint(5, 15)
    vehicle_details = random.choice(["suv", "sedan","hatchback","minivan"])
    trip_duration = random.randint(10, 60)
    trip_distance = random.uniform(1, 20)
    trip_rating =  random.uniform(1.0, 5.0)
    trip_id = fake.uuid4()



    # Derive time_of_day and day_of_week from booking_timestamp
    booking_hour = booking_timestamp.hour
    if 6 <= booking_hour < 9:
        time_of_day = "morning"
    elif 9 <= booking_hour < 12:
        time_of_day = "forenoon"
    elif 12 <= booking_hour < 17:
        time_of_day = "afternoon"
    elif 17 <= booking_hour < 21:
        time_of_day = "evening"
    else:
        time_of_day = "night"

    day_of_week = booking_timestamp.strftime("%A")  # Get the day of the week

    weather_conditions = random.choice(["clear", "rainy", "snowy", "cloudy","sunny","windy"])
    traffic_conditions = random.choice(["light", "moderate", "heavy",'normal'])
    passenger_pick_up_time = booking_timestamp + pd.Timedelta(minutes=random.randint(1, 30))

    # Calculate the difference in seconds between the current time and the booking timestamp

    # Generate a random additional waiting time before cancellation (1 to 10 minutes)
    additional_waiting_time = random.randint(1, 10)

    # Introduce patterns for cancellations
    if (drivers_review < 2 or estimated_arrival_time > (estimated_arrival_time + additional_waiting_time) or
        (time_of_day == "morning" and 6.5 <= booking_timestamp.hour <= 9 and
        estimated_arrival_time > random.randint(1, 10))
    ) and (weather_conditions == "rainy" or
            time_of_day == "night" or
            day_of_week in ["Saturday", "Sunday"] or
            booking_source == "app"):
        status = "cancelled"
        cancelled_time = booking_timestamp + pd.Timedelta(minutes=random.randint(1, 10))
        # Order completion time for cancellations
        order_completion_time = cancelled_time
        passenger_pick_up_time = None
        trip_duration = None
        trip_distance = None
    else:
        status = "completed"
        cancelled_time = None
        order_completion_time = booking_timestamp + pd.Timedelta(minutes=trip_duration)
        passenger_pick_up_time = booking_timestamp + pd.Timedelta(minutes=(estimated_arrival_time + random.randint(-5,5)))
        trip_duration = order_completion_time - passenger_pick_up_time

    keys = ["user_id", "taxi_id", "driver_id","passenger_current_lat","passenger_current_lon","passenger_arrival_lat", "passenger_arrival_lon",
            "drivers_current_lat", "drivers_current_lon", "drivers_location", "pickup_location", "destination_location", "drivers_review",
                                "booking_timestamp", "booking_source", "n_passengers", "estimated_fare",
                                "estimated_arrival_time_in_m", "vehicle_details", "trip_duration", "trip_distance","trip_rating",
                                'trip_id',"time_of_day", "day_of_week", "weather_conditions", "traffic_conditions",
                                "passenger_pick_up_time", "status", "order_completion_time"]

    values = [user, taxi, driver,passenger_current_lat, passenger_current_lon,passenger_arrival_lat, passenger_arrival_lon, drivers_current_lat, drivers_current_lon,drivers_location,pickup_location, destination_location, drivers_review, str(booking_timestamp), booking_source,
                n_passengers, estimated_fare, estimated_arrival_time, vehicle_details, str(trip_duration), trip_distance,trip_rating,
                trip_id,time_of_day, day_of_week, weather_conditions, traffic_conditions, str(passenger_pick_up_time),
                status, str(order_completion_time)]
    my_dict = dict(zip(keys, values))
    return my_dict

def generate_entries(data_list):
    data_list.clear()
    for _ in range(20):
        data_list.append(generate_fake_taxi_data())

# Set up a route to access the generated data
@app.route('/fake_taxi_data', methods=['GET'])
def get_fake_taxi_data():
    return jsonify(fake_taxi_data_list)


# Create a scheduler to regenerate data every 20 seconds
scheduler = BackgroundScheduler()
scheduler.add_job(generate_entries, 'interval', seconds=40, args=[fake_taxi_data_list])
scheduler.start()

if __name__ == '__main__':
    generate_entries(fake_taxi_data_list)  # Generate initial data
    app.run(host='localhost', port=8081)

