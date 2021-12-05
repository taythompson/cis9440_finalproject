# Air Pollution in NYC 
- author(s): Jack Hazan + Taylor Thompson
- date created: 12/5/2021
- class: CIS 9440

Project Objective: Follow the Kimball Lifecycle to design and develop a public, cloud-based Data Warehouse with a functioning BI Applications

Project Tools:
The tools used to build this Data Warehouse were: (change this to make applicable to your project)
1. For data integration - python
2. For data warehousing - Google BigQuery
3. For Business Intelligence - Tableau

## Kimball Lifecycle Project Stages

### Project Planning

Motivation for project:

Determine the key factors that impact the air quality in nyc neighborhoods and what factors can improve the air quality in nyc neighborhoods.

Description of the issues or opportunities the project will address:

As part of our corporate social responsibility, there is a strong need to improve the air quality in NYC neighborhoods. Exposure to air pollutants affects all NYC residents alike but there are certain neighborhoods that have significantly worse air quality in comparison to other neighborhoods. To address the looming concerns over NYC Air Quality, we need to gain a better understanding we need to gain a better understanding of the answers to the following questions: 

How does the increase in population impact air quality?
How does the number of subway stations impact air quality?
Does the amount of trees in a neighborhood have a positive or negative impact on air quality?
How does the traffic density in a neighborhood impact greenhouse gas emissions? 

NYC Department of Environmental Protection aims to answer these questions by utilizing air quality, tree census, population census, greenhouse gas emissions, subway station and traffic data. These datasets will provide valuable insight as to how we can improve the air quality in New York City.



Project Business or Organization Value:

By analyzing the factors that impact air quality in New York City, we will be able to derive a more strategic plan to improve air quality for the coming years. Our mission as the Department of Environmental Protection is to provide a sustainable future for all New Yorkers.


Data Sources:
1. https://data.cityofnewyork.us/Environment/Air-Quality/c3uy-2p5r
2. https://data.cityofnewyork.us/Transportation/Subway-Stations/arq3-7z49
3. https://data.cityofnewyork.us/Environment/2015-Street-Tree-Census-Tree-Data/uvpi-gqnh/data
4. https://data.cityofnewyork.us/City-Government/New-York-City-Population-by-Borough-1950-2040/xywu-7bv9/data
5. https://data.beta.nyc/en/dataset/pediacities-nyc-neighborhoods/resource/7caac650-d082-4aea-9f9b-3681d568e8a5


### Business Requirements Definition

List of Data Warehouse KPI's:
1. Air Pollultion Levels in NYC YoY
2. Number of Trees Per Borough
3. Total Population Per Borough
4. Total Amount of Subway Stations Per Borough 
5. Traffic Congestion Level Per Borough


### Dimensional Model

This project's Dimensional Model consists of (x) Facts and (y) Dimensions


<img width="938" alt="Screen Shot 2021-12-05 at 9 27 10 AM" src="https://user-images.githubusercontent.com/48069159/144750999-6c13dba4-3164-4bbf-98fc-8dfd9ecc4ad0.png">

This project's Kimball Bus Matrix:


<img width="621" alt="Screen Shot 2021-12-05 at 9 26 55 AM" src="https://user-images.githubusercontent.com/48069159/144751016-73f51b46-f7ce-4b0f-ad5b-29147c10abe0.png">


### Business Intelligence Design and Development

List of Visualizations for each KPI:
1. Line Chart for comparison of Air Pollution levels in NYC from the years 2015 - 2018
2. Map Chart for comparison of the total number of healthy trees in each zip code
3. Horizontal Bar Chart for comparison of the amount of population per zip code, which will show how a densely populated borough may contribute to air pollution.
4. Map Chart for comparison of how many train stations per borough and how it contributes to air pollution.
5. Heat Map for comparison of how the density of traffic per borough contributes to air pollution.
...

BI Application Wireframe design:


![image](https://user-images.githubusercontent.com/48069159/144750716-a850e708-f976-423e-ab84-1fb0f160ecf5.png)

Picture of final Dashboard:


<img width="501" alt="Screen Shot 2021-12-05 at 9 30 53 AM" src="https://user-images.githubusercontent.com/48069159/144750853-423a326b-18ef-443d-9d14-0f3b1145a532.png">

### Deployment

The project was deployed on Tableau Public: https://public.tableau.com/app/profile/jack.hazan/viz/Milestone4_16384042887970/Dashboard1?publish=yes
