**YouTube Data Harvesting and Warehousing**

**Overview**

This project aims to develop a user-friendly Streamlit application that allows users to access and analyze data from multiple YouTube channels. The application utilizes the Google API to extract information on a YouTube channel, stores it in a MongoDB database, migrates it to a SQL data warehouse, and enables users to search for channel details and join tables to view data in the Streamlit app.

**Features**

1.	Retrieve Channel Data: Users can input a YouTube channel ID and retrieve relevant data such as channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, and comments for each video.
2.	Store Data in MongoDB: Option to store retrieved data in a MongoDB database as a data lake for easy access and storage.
3.	Collect Data for Multiple Channels: Ability to collect data for up to 10 different YouTube channels and store them in the MongoDB data lake by clicking a button.
4.	Migrate Data to SQL Database: Option to select a channel name and migrate its data from the MongoDB data lake to a SQL database as tables for further analysis.
5.	Search and Retrieve Data: Users can search and retrieve data from the SQL database using different search options, including joining tables to get channel details.

**Technologies Used**

•	Streamlit: Used to build the user interface for the application.

•	Google API Client Library for Python: Used to retrieve data from the YouTube API.


•	MongoDB: Used as a data lake to store unstructured and semi-structured data.

•	SQL Database (e.g., MySQL, PostgreSQL): Used as a data warehouse to store structured data for efficient querying.


•	Python SQL Libraries (e.g., SQLAlchemy): Used to interact with the SQL database.

•	GitHub: Used for version control and collaboration.

**Usage**

1.	Clone the repository to your local machine.
2.	Install the required dependencies using pip install -r requirements.txt.
3.	Set up the necessary API credentials for Google API.
4.	Run the Streamlit application using streamlit run app.py.
5.	Input a YouTube channel ID and retrieve relevant data.
6.	Store the data in MongoDB or migrate it to a SQL database as needed.
7.	Search and retrieve data from the SQL database using different search options.
