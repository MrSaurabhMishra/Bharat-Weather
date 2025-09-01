import pandas as pd
import requests
from sqlalchemy import create_engine
import time
import pymysql

# MySQL database connection
db_user = "root"
db_password = "Saurabh1042"
db_host = "localhost"
db_port = "3306"
db_name = "weather_data"
table_name = "weather"

# Create SQLAlchemy engine
engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}")

# Cities / districts list
cities = [
    "Sultanpur", "Lucknow", "Kanpur", "Varanasi", "Allahabad",
    "Surat", "Mumbai", "Bangalore", "Chennai", "Kolkata",
    "Nagpur", "Hyderabad", "Bhopal", "Indore", "Jabalpur",
    "Ujjain", "Gwalior", "Raipur", "Bilaspur", "Jalandhar",
    "Jodhpur", "Patiala", "Ludhiana", "Nainital", "Shimla",
    "Srinagar", "Gangtok", "Kathmandu", "Pokhara", "Ranchi",
    "Bhavnagar", "Ahmedabad", "Vadodara", "Jamnagar", "Nadiad",
    "Anand", "Rajkot", "Bhiwani", "Moradabad", "Gandhinagar",
    "Rajasthan", "Gurugram", "Noida", "Faridabad", "Saharanpur",
    "Sikar", "Udaipur", "Jaipur", "Kota", "Bikaner", "Rohtak",
    "Patna", "Prayagraj", "Ayodhya", "Madhubani","New Delhi",
    "Washington D.C.","London","Ottawa","Canberra","Berlin",
    "Paris","Rome","Tokyo","Beijing","Moscow","Brasília","Pretoria",
    "Cairo","Mexico City","Buenos Aires","Riyadh","Ankara","Jakarta",
    "Kathmandu","Sri Jayawardenepura Kotte","Islamabad","Dhaka","Singapore",
    "Abu Dhabi","Kuala Lumpur","Bangkok","Seoul","Hanoi","Baghdad","Tehran",
    "Damascus","Riyadh","Doha","Muscat","Manama","Amman","Beirut","Minsk",
    "Warsaw","Prague","Vienna","Brussels","Amsterdam","Oslo","Stockholm",
    "Helsinki","Copenhagen","Lisbon","Athens","Budapest","Sofia","Belgrade"

]
# API key
api_key = "8d00bcea26774a26bdd122850251708"
# Function to fetch weather data
def fetch_weather():
    weather_list = []
    for city in cities:
        url = f"https://api.weatherapi.com/v1/current.json?key={api_key}&q={city}"
        try:
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                weather_info = {
                    "city": data["location"]["name"],
                    "region": data["location"]["region"],
                    "country": data["location"]["country"],
                    "tz_id": data["location"]["tz_id"],
                    "local_time": data["location"]["localtime"],
                    "temp_c": data["current"]["temp_c"],
                    "condition": data["current"]["condition"]["text"],
                    "wind_kph": data["current"]["wind_kph"]
                }
                weather_list.append(weather_info)
            else:
                print(f"Failed for {city}: {response.status_code}")
        except Exception as e:
            print(f"Error for {city}: {e}")
        time.sleep(1)  # Respect API rate limit
    
    return pd.DataFrame(weather_list)

# Fetch and save weather data to a CSV file
weather_df = fetch_weather()
weather_df.to_csv("weather_data.csv", index=False)

 # Update MySQL table with latest weather data from fetched data frame
def update_mysql(df):
    # Replace table with new data
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    print(f"MySQL table '{table_name}' updated with latest weather data.")
    
update_mysql(weather_df)


import matplotlib.pyplot as plt
import seaborn as sns

# visualizations

# Temperature over time
sns.lineplot(data=weather_df, x='local_time', y='temp_c')
plt.title('Temperature over Time')
plt.xlabel('Time')
plt.ylabel('Temperature (°C)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('temperature_over_time.png')
plt.show()

# Temperature by City
sns.barplot(data=weather_df, x='city', y='temp_c')
plt.title('Temperature by City')
plt.xlabel('City')
plt.ylabel('Temperature (°C)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('temperature_by_city.png')
plt.show()

# Temperature by Weather Condition
sns.barplot(data=weather_df, x='condition', y='temp_c')
plt.title('Temperature by Weather Condition')
plt.xlabel('Weather Condition')
plt.ylabel('Temperature (°C)')
plt.xticks(rotation=45)
plt.tight_layout() 
plt.savefig('temperature_by_condition.png')
plt.show()

# Descriptive statistics for numeric columns
numeric_weather_df = weather_df.select_dtypes(include='number')
descriptive_stats = numeric_weather_df.describe()

# Correlation heatmap
plt.figure(figsize=(10, 6))
sns.heatmap(numeric_weather_df.corr(), annot=True, fmt=".2f", cmap="coolwarm")
plt.title('Correlation Heatmap')
plt.tight_layout()
plt.savefig('correlation_heatmap.png')
plt.show()

# Additional visualizations
# Wind speed by city
sns.barplot(data=weather_df, x='city', y='wind_kph')
plt.title('Wind Speed by City')
plt.xlabel('City')
plt.ylabel('Wind Speed (kph)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('wind_speed_by_city.png')
plt.show()

# Temperature vs Wind Speed
sns.scatterplot(data=weather_df, x='temp_c', y='wind_kph')
plt.title('Temperature vs Wind Speed')
plt.xlabel('Temperature (°C)')
plt.ylabel('Wind Speed (kph)')
plt.tight_layout()
plt.savefig('temperature_vs_wind_speed.png')
plt.show()

# Correlation analysis
correlation = numeric_weather_df.corr()
print(correlation)

# Group by local_time and calculate mean temperature
temperature_trends = weather_df.groupby('local_time')['temp_c'].mean().reset_index()

# Visualize temperature trends over time
sns.lineplot(data=temperature_trends, x='local_time', y='temp_c')
plt.title('Temperature Trends Over Time')
plt.xlabel('Time')
plt.ylabel('Temperature (°C)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig('temperature_trends_over_time.png')
plt.show()

# Analyze temperature distribution
sns.histplot(data=weather_df, x='temp_c', bins=30, kde=True)
plt.title('Temperature Distribution')
plt.xlabel('Temperature (°C)')
plt.ylabel('Frequency') 
plt.tight_layout()
plt.savefig('temperature_distribution.png')
plt.show()

# Conclusion
# The weather data analysis has provided valuable insights into temperature and wind patterns.
# Further analysis can be conducted to explore other aspects of the data.

# These insights can help in understanding weather trends and making informed decisions.
# These findings can be used to improve weather forecasting models and enhance our understanding of climate patterns.
 