library(dplyr)

#upload the data for vehicles
vehicles = read.csv('/Users/tony/Desktop/Applied_Analytics/APAN5310/Final/data/vehicles.csv')

#select necessary information and impute missing values 
#from 426880 observations with 26 variables to 421344 observations with 11 variables
vehicles <- vehicles %>% select("id", "state","year","manufacturer","model","condition","cylinders","fuel","odometer","paint_color","type")
vehicles <- na.omit(vehicles)
vehicles$state <- toupper(vehicles$state)

#mock data with car_rental_companies and cars
#mock country = United States, since all the state here are in the USA
vehicles <- vehicles %>% mutate(country = "United States")

#mock address
address = read.csv('/Users/tony/Desktop/Applied_Analytics/APAN5310/Final/data/list_of_real_usa_addresses.csv')
cars <- left_join(vehicles, address, by = "state")
library(tidyr)
cars = distinct(cars, id, .keep_all = TRUE)

#car_rental_companies table
car_rental_companies <- cars %>% 
  select("address","city","state","country","zip")  %>%
  unique() %>%
  mutate(name=sample(paste0("Company ", 1:51), size = n(), replace = TRUE)) %>%
  mutate(car_company_id=seq(1, 51))

#cars table
cars$id <- 1:421344
cars <- cars %>% 
  left_join(car_rental_companies, by = c("state")) %>% 
  select("id","car_company_id","year","manufacturer","model","condition","cylinders","fuel","odometer","paint_color","type") %>%
  unique()

#mock rent_reservations table
rent_reservations <- data.frame(reservation_id = seq(1, 100), 
                           guest_id = sample(1:1000, 100, replace = TRUE),
                           date = sample(seq(as.Date('2022/01/01'), as.Date('2022/12/31'), by="day"), 100, replace = FALSE),
                           duration = sample(1:14, 100, replace = TRUE),
                           car_ids <- sample(cars$id, 50, replace = TRUE))
car_ids <- sample(cars$id, 50, replace = TRUE)
rent_reservations$car_id <- sample(cars$id, 50, replace = TRUE)
car_prices=sample(20:40, 50, replace = TRUE)
rent_reservations$price <- car_prices * rent_reservations$duration
rent_reservations <- rent_reservations %>% select("reservation_id","guest_id","car_id","date","duration","price")

#mock rental_reviews table
rental_reviews <- rent_reservations[49:100,] %>% 
  select("reservation_id","guest_id","date","duration") %>%
  mutate(review_date = as.Date(date) + duration) %>%
  select(-"date",-"duration") %>%
  mutate(reviews = sample(c("Great experience!", "Good service", "Average", "Poor experience"), 52, replace = TRUE)) %>%
  mutate(rating_score = ifelse(reviews == "Great experience!", 5,
                               ifelse(reviews == "Good service", 4,
                                      ifelse(reviews == "Average", 3, 
                                             sample(1:2, 1, replace = TRUE)))))



#load the PostgreSQL driver
require('RPostgreSQL')
drv <- dbDriver('PostgreSQL')

#create a connection
library("DBI")
library("RPostgres")
library('remotes')
con <- DBI::dbConnect(
  RPostgres::Postgres(),
  dbname = 'Final', 
  host = 'localhost', 
  port = 5432,
  user = 'postgres', 
  password = '123')


#0. all_reserve
t_all_reserve <- "CREATE TABLE all_reserve (
  reservation_id INT PRIMARY KEY,
  pay_method VARCHAR(20),
  amount DECIMAL(10,2)
);"
dbGetQuery(con,t_all_reserve)
#mock all_reserve table
n_rows <- 1000
mock_data <- data.frame(
  reservation_id = 1:n_rows,
  pay_method = sample(c("credit", "debit"), n_rows, replace = TRUE),
  amount = round(runif(n_rows, min = 5000, max = 50000), 2)
)
# Insert mock data into all_reserve table
dbWriteTable(con,name="all_reserve",value=mock_data,row.names=FALSE, append=TRUE)


#1. guests
#create the table guests
t_guests <- "CREATE TABLE guests (
  guest_id INT PRIMARY KEY,
  username VARCHAR(50),
  first_name VARCHAR(50),
  last_name VARCHAR(50),
  age INT,
  gender VARCHAR(10),
  nationality VARCHAR(50),
  email VARCHAR(100),
  phone VARCHAR(20) NOT NULL
); "
dbGetQuery(con,t_guests)
#mock the table and insert data
# Create a mock data frame for guests
guests <- data.frame(
  guest_id = 1:1000,
  username = paste0("guest", 1:1000),
  first_name = paste0("first_name", 1:1000),
  last_name = paste0("last_name", 1:1000),
  age = sample(18:80, 1000, replace = TRUE),
  gender = sample(c("Male", "Female", "Other"), 1000, replace = TRUE),
  nationality = sample(c("USA", "Canada", "Mexico", "UK", "France", "Germany"), 1000, replace = TRUE),
  email = paste0("guest", 1:1000, "@example.com"),
  phone = sample(c("123-456-7890", "555-123-4567", "888-555-1234"), 1000, replace = TRUE)
)
dbWriteTable(con,name="guests",value=guests,row.names=FALSE, append=TRUE)

#2.hotels
#upload the data for hotels
Datafiniti_Hotel_Reviews = read.csv('/Users/tony/Desktop/Applied_Analytics/APAN5310/Final/data/Datafiniti_Hotel_Reviews.csv')
t_hotels <- "CREATE TABLE hotels (
  hotel_id INT PRIMARY KEY,
  name VARCHAR(100),
  address VARCHAR(100),
  city VARCHAR(50),
  state VARCHAR(50),
  country VARCHAR(50),
  zip_code VARCHAR(10),
  stars INT
);"
dbGetQuery(con,t_hotels)

hotels <- unique(Datafiniti_Hotel_Reviews[c("id","name","address","city","province","country","postalCode")])
hotels <- hotels %>% mutate(stars=sample(2:5, 1853,replace = TRUE))
replace_ids <- function(df1, df2, mapping_table) {
  df1 <- df1 %>% left_join(mapping_table, by = c("id" = "old_id")) %>% 
    mutate(id = ifelse(!is.na(new_id), new_id, id)) %>% 
    select(-new_id)
  df2 <- df2 %>% left_join(mapping_table, by = c("id" = "old_id")) %>% 
    mutate(id = ifelse(!is.na(new_id), new_id, id)) %>% 
    select(-new_id)
  return(list(df1 = df1, df2 = df2))
}
mapping_table <- data.frame(
  old_id = hotels$id,
  new_id = 1:length(hotels$id)
)
hotel_reviews <- unique(Datafiniti_Hotel_Reviews[c("id","reviews.date","reviews.text","reviews.rating")])
hotel_reviews <- hotel_reviews %>% mutate(guest_id=sample(1:1000, 9996,replace = TRUE))
hotel_reviews <- hotel_reviews %>% select("id","guest_id","reviews.date","reviews.text","reviews.rating")
updated_dfs <- replace_ids(hotels, hotel_reviews, mapping_table)
hotels <- updated_dfs$df1 %>% select("id","name","address","city","province","country","postalCode","stars")
names(hotels) <- c("hotel_id","name","address","city","state","country","zip_code","stars")
dbWriteTable(con,name="hotels",value=hotels,row.names=FALSE, append=TRUE)

#3.rooms
t_rooms <- "CREATE TABLE rooms (
  room_id INT PRIMARY KEY,
  room_no INT,
  hotel_id INT,
  room_type VARCHAR(50),
  occupancy int,
  FOREIGN KEY (hotel_id) REFERENCES hotels(hotel_id)
);"
dbGetQuery(con,t_rooms)
rooms <- data.frame(
  room_id = 1:3000,
  room_no = paste0(c(1:30), sprintf("%02d", c(1:8))),
  hotel_id = sample(1:1853, 3000, replace = TRUE),
  room_type = sample(c("single", "double", "twin", "suite","deluxe"), 3000, replace = TRUE),
  occupancy = sample(1:6, 3000, replace = TRUE)
)
dbWriteTable(con,name="rooms",value=rooms,row.names=FALSE, append=TRUE)

#4. hotel_reservations
t_hotel_reservations <- "CREATE TABLE hotel_reservations (
  id SERIAL PRIMARY KEY,
  reservation_id INT NOT NULL,
  guest_id INT,
  room_id INT,
  date DATE,
  nights INT,
  prices DECIMAL(10,2),
  FOREIGN KEY (guest_id) REFERENCES guests(guest_id),
  FOREIGN KEY (room_id) REFERENCES rooms(room_id),
  FOREIGN KEY (reservation_id) REFERENCES all_reserve(reservation_id)
);"
dbGetQuery(con,t_hotel_reservations)
hotel_reservations <- data.frame(
  reservation_id = sample(1:1000, 1000, replace = TRUE),
  guest_id = sample(1:1000, 1000, replace = TRUE),
  room_id = sample(rooms$room_id, 1000, replace = TRUE),
  date = sample(seq(as.Date('2017/01/01'), as.Date('2022/12/31'), by="day"), 1000, replace = FALSE),
  nights = sample(1:8, 1000, replace = TRUE),
  prices = sample(100:700, 1000, replace = TRUE)
)
dbWriteTable(con,name="hotel_reservations",value=hotel_reservations,row.names=FALSE, append=TRUE)

#5. hotel_reviews
t_hotel_reviews <-"CREATE TABLE hotel_reviews (
  hotel_id INT,
  guest_id INT,
  date TIMESTAMP,
  reviews TEXT,
  rating_score DECIMAL(2,1),
  PRIMARY KEY (hotel_id,guest_id, date),
  FOREIGN KEY (hotel_id) REFERENCES hotels(hotel_id),
  FOREIGN KEY (guest_id) REFERENCES guests(guest_id));"
dbGetQuery(con,t_hotel_reviews)
hotel_reviews <- updated_dfs$df2 %>% select("id","guest_id","reviews.date","reviews.text","reviews.rating")
names(hotel_reviews) <- c("hotel_id","guest_id","date","reviews","rating_score")
hotel_reviews <- hotel_reviews %>% 
  group_by(hotel_id, guest_id, date) %>% 
  summarize(reviews = first(reviews), rating_score = first(rating_score))
dbWriteTable(con,name="hotel_reviews",value=hotel_reviews,row.names=FALSE, append=TRUE)

#6. Airline_Company
# create company_info table
data = read.csv('/Users/tony/Desktop/Applied_Analytics/APAN5310/Final/data/Clean_Dataset.csv')
company_info <- data.frame(
  name = c("SpiceJet","AirAsia","Vistara","GO_FIRST","Indigo","Air_India"),
  address = c("319 Udyog Vihar","Jalan KLIA S3, Southern Support Zone","Jeevan Bharti Tower 1",
              "8th Floor, DLF Cyber City, Phase II ","Mehrauli-Gurgaon Road",
              "113 Gurudwara Rakabganj Road" ),
  city = c("Gurgaon","Sepang"," New Delhi","Gurgaon","Gurgaon","New Delhi"),
  state = c("Haryana","Selangor Darul Ehsan","Delhi","Haryana","Haryana","Delhi"),
  country = c("India","Malaysia","India","India","India","India"),
  zipcode = c("122016","64000","110001","122002","122002","110001")
)
#select necessary information and mock airline_company table
airlines <- data %>% 
  select("airline")%>%
  rename(name =airline)
Airlines_company <- airlines %>%
  left_join(company_info, by = "name")

t_Airline_Company <- "CREATE TABLE airline_company (
  airline_company_id INT PRIMARY KEY,
  name VARCHAR(100),
  address VARCHAR(100),
  city VARCHAR(50),
  state VARCHAR(50),
  country VARCHAR(50),
  zip_code VARCHAR(10)
);"
dbGetQuery(con,t_Airline_Company)
Airline_Company <- unique(Airlines_company)
Airline_Company <- Airline_Company %>% mutate(airline_company_id = 1:6) %>%
  select(c("airline_company_id", "name", "address", "city", "state", "country", "zipcode"))
names(Airline_Company) <- c("airline_company_id", "name", "address", "city", "state", "country", "zip_code")
dbWriteTable(con,name="airline_company",value=Airline_Company,row.names=FALSE, append=TRUE)

#7. planes
t_planes <-"CREATE TABLE planes (
  plane_id INT PRIMARY KEY,
  plane_type VARCHAR(50),
  flying_num INT,
  passenger_num INT,
  airline_company_id INT,
  FOREIGN KEY (airline_company_id) REFERENCES airline_company(airline_company_id)
);"
dbGetQuery(con,t_planes)

planes <- data.frame(flying_num = sample(50:2000, size = 50, replace = TRUE),
                     plane_type = sample(c("A320", "A330", "A350", "A319", "B737", "B777", "B747", "B737Max"), size = 50, replace = TRUE),
                     passenger_num = sample(50:400, size = 50, replace = TRUE),
                     airline_company_id = sample(1:6,50,replace = TRUE))
planes <- planes %>% mutate(plane_id=1:50)
planes <- planes %>% select(c("plane_id","plane_type","flying_num","passenger_num",
                      "airline_company_id"))
dbWriteTable(con,name="planes",value=planes,row.names=FALSE, append=TRUE)

#8. flights
t_flights <- "CREATE TABLE flights (
  flight_id INT PRIMARY KEY,
  flight_no VARCHAR(20),
  passenger_id INT,
  plane_id INT,
  departure VARCHAR(100),
  destination VARCHAR(100),
  date DATE,
  take_off TIME,
  landing TIME,
  duration FLOAT,
  FOREIGN KEY (passenger_id) REFERENCES guests(guest_id),
  FOREIGN KEY (plane_id) REFERENCES planes(plane_id)
);"
dbGetQuery(con,t_flights)

f = read.csv('/Users/tony/Desktop/Applied_Analytics/APAN5310/Final/data/Clean_Dataset.csv')
f <- f %>%
  select("flight","destination_city","duration")%>%
  rename(flight_no = flight,
         destination = destination_city)%>%
  slice(1:50)
start_time <- as.POSIXct("00:00", format = "%H:%M", tz = "UTC")
end_time <- as.POSIXct("23:59", format = "%H:%M", tz = "UTC")
times <- seq(from = start_time, to = end_time, by = "1 min")
time_only <- format(times, format = "%H:%M:%S", usetz = FALSE)
flights <- f %>%
  mutate(departure = sample(c("New York", "Delhi", "Bangalore", "Hongkong", "Bangkok", "Istanbul", "Singapore", "Abu Dhabi"), size = nrow(f), replace = TRUE),
         date = sample(seq(as.Date('2022/01/01'), as.Date('2022/12/31'), by="day"), size = nrow(f), replace = FALSE),
         take_off = sample(time_only, size = nrow(f), replace = TRUE),
         landing = sample(time_only, size = nrow(f), replace = TRUE),
         flight_id=1:50,
         passenger_id=sample(1:1000,50,replace=FALSE),
         plane_id=sample(1:50,50,replace=FALSE))
flights<- flights %>% select("flight_id","flight_no","passenger_id","plane_id",
                             "departure","destination","date","take_off","landing","duration")
dbWriteTable(con,name="flights",value=flights,row.names=FALSE, append=TRUE)


#9. ticket_prices
t_ticket_prices <-"CREATE TABLE ticket_prices (
  ticket_id INT PRIMARY KEY,
  reservation_id INT NOT NULL,
  flight_id INT,
  classes VARCHAR(50),
  prices DECIMAL(10,2),
  FOREIGN KEY (flight_id) REFERENCES flights(flight_id),
  FOREIGN KEY (reservation_id) REFERENCES all_reserve(reservation_id)
);"
dbGetQuery(con,t_ticket_prices)
ticket_prices = read.csv('/Users/tony/Desktop/Applied_Analytics/APAN5310/Final/data/ticket_prices.csv')
##
ticket_prices$reservation_id=reservation_id=sample(1:1000, size = 50, replace = FALSE)
ticket_prices <- ticket_prices %>% mutate(ticket_id=1:50,
                                          flight_id=sample(1:50, size = 50, replace = TRUE)) %>% 
  select("ticket_id", "reservation_id", "flight_id", "classes", "prices")
dbWriteTable(con,name="ticket_prices",value=ticket_prices,row.names=FALSE, append=TRUE)

#10. flight_reviews
t_flight_reviews <- "CREATE TABLE flight_reviews (
  flight_id INT,
  passenger_id INT,
  date DATE,
  reviews TEXT,
  rating_score DECIMAL(2,1),
  PRIMARY KEY (flight_id,passenger_id,date),
  FOREIGN KEY (flight_id) REFERENCES flights(flight_id),
  FOREIGN KEY (passenger_id) REFERENCES guests(guest_id)
); "
dbGetQuery(con,t_flight_reviews)
flight_reviews <- data.frame(
  reviews = sample(c("Great experience!", "Good service", "Average", "Poor experience"), size = 50, replace = TRUE),
  passenger_id = sample(1:1000,50, replace = FALSE),
  date=sample(seq(as.Date('2022/01/01'), as.Date('2022/12/31'), by="day"), 50, replace = FALSE)) %>%
  mutate(rating_score = if_else(reviews == "Great experience!", 5,
                                if_else(reviews == "Good service", 4,
                                        if_else(reviews == "Average", 3,
                                                if_else(reviews == "Poor experience", 1, NA_real_))))) %>%
  mutate(flight_id = 1:50) 
flight_reviews <- flight_reviews %>% select("flight_id","passenger_id","date","reviews","rating_score")
dbWriteTable(con,name="flight_reviews",value=flight_reviews,row.names=FALSE, append=TRUE)

#11. car_rental_companies
t_car_rental_companies <- "CREATE TABLE car_rental_companies (
  car_company_id INT PRIMARY KEY,
  name VARCHAR(100),
  address VARCHAR(100),
  city VARCHAR(50),
  state VARCHAR(50),
  country VARCHAR(50),
  zip_code VARCHAR(10)
);"
dbGetQuery(con,t_car_rental_companies)
car_rental_companies <- car_rental_companies %>% select("car_company_id", "name", "address", "city","state","country","zip")
names(car_rental_companies) <- c("car_company_id", "name", "address", "city","state","country","zip_code")
dbWriteTable(con,name="car_rental_companies",value=car_rental_companies,row.names=FALSE, append=TRUE)


#12. cars
t_cars <- "CREATE TABLE cars (
  car_id BIGINT PRIMARY KEY,
  car_company_id INT,
  year INT,
  manufacturer VARCHAR(255),
  model VARCHAR(255),
  condition VARCHAR(255),
  cylinders VARCHAR(255),
  fuel VARCHAR(255),
  odometer INT,
  paint_color VARCHAR(255),
  type VARCHAR(255),
  FOREIGN KEY (car_company_id) REFERENCES car_rental_companies(car_company_id)
); "
dbGetQuery(con,t_cars)
names(cars) <- c("car_id", "car_company_id", "year","manufacturer","model","condition","cylinders","fuel","odometer","paint_color","type")
dbWriteTable(con,name="cars",value=cars,row.names=FALSE, append=TRUE)

#13. rent_reservations
t_rent_reservations <- "CREATE TABLE rent_reservations (
  id SERIAL PRIMARY KEY,
  reservation_id INT NOT NULL,
  guest_id INT,
  car_id BIGINT,
  date DATE,
  duration INT,
  prices DECIMAL(10,2),
  FOREIGN KEY (guest_id) REFERENCES guests(guest_id),
  FOREIGN KEY (car_id) REFERENCES cars(car_id),
  FOREIGN KEY (reservation_id) REFERENCES all_reserve(reservation_id)
);"
dbGetQuery(con,t_rent_reservations)
names(rent_reservations) <- c("reservation_id", "guest_id", "car_id","date","duration","prices")

dbWriteTable(con,name="rent_reservations",value=rent_reservations,row.names=FALSE, append=TRUE)

#14. rental_reviews
t_rental_reviews <- "CREATE TABLE rental_reviews (
  guest_id INT,
  car_id BIGINT,
  date DATE,
  reviews TEXT,
  rating_score INT,
  PRIMARY KEY (guest_id,car_id,date),
  FOREIGN KEY (guest_id) REFERENCES guests(guest_id)
); "
dbGetQuery(con,t_rental_reviews)
names(rental_reviews) <- c("guest_id", "car_id", "date", "reviews","rating_score")                            
dbWriteTable(con,name="rental_reviews",value=rental_reviews,row.names=FALSE, append=TRUE)

#15. user_loyalty
t_user_loyalty <- "CREATE TABLE user_loyalty (
  guest_id INT PRIMARY KEY,
  loyalty_group VARCHAR(50),
  activity DECIMAL(10,2),
  FOREIGN KEY (guest_id) REFERENCES Guests(guest_id)
);"
dbGetQuery(con,t_user_loyalty)
user_loyalty <- data.frame(guest_id = 1:1000,
                           loyalty_group = sample(c("gold", "diamond", "platinum", "silver"), 1000, replace = TRUE),
                           activity = sample(1:100, 1000,replace = TRUE))
dbWriteTable(con,name="user_loyalty",value=user_loyalty,row.names=FALSE, append=TRUE)






