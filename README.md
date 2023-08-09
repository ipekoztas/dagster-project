# dagster-project
Implemented ETL pipeline using Dagster
Project Title: Data Analysis and ETL Automation with Dagster

This GitHub project exemplifies the creation of an end-to-end ETL (Extract, Transform, Load) pipeline using Dagster, combined with web scraping, data analysis, and visualization. The project concentrates on extracting specific data from a NOAA (National Centers for Environmental Information) website, performing in-depth analysis utilizing Pandas, Plotly, NumPy, and Scikit-learn in a Jupyter Notebook environment. Moreover, the ETL process is orchestrated and automated with Dagster, offering reliability and repeatability, and optionally, the processed data can be saved into a PostgreSQL table using another dedicated pipeline.

Key Components:

Data Extraction with Web Scraping: The project extracts data from the NOAA website, particularly focusing on local climatological information for a given year.

ETL Pipeline using Dagster: The ETL process is orchestrated with Dagster, a data orchestrator tool. It involves extracting the data, performing necessary transformations, and loading it into various targets, including local analysis and optionally, a PostgreSQL database.

Data Analysis and Visualization: Extracted data is analyzed and visualized using Pandas for data manipulation, Plotly for interactive visualizations, NumPy for numerical computations, and Scikit-learn for regression analysis.

Automated Jupyter Notebook Execution: Dagster automates the execution of Jupyter Notebook scripts, ensuring consistent analysis and reporting without manual intervention.

PostgreSQL Integration: An additional Dagster pipeline enables saving processed data to a PostgreSQL table, creating a structured and accessible repository for future analysis.

Configuration Management: The project incorporates configuration management to handle parameters and settings for the ETL pipeline, ensuring flexibility and adaptability.

Getting Started:

Clone the repository and navigate to the project directory.
Set up the required environment using the provided requirements file.
Configure the necessary parameters for the ETL pipeline, such as the desired year for data extraction.
Run the Dagster ETL pipeline to automate data extraction, transformation, and loading.
Explore the generated Jupyter Notebook for comprehensive data analysis and visualization.
Optionally, run the PostgreSQL pipeline to store processed data in a structured database.
Project Significance:

This project stands as a model implementation of ETL automation and data analysis, combining powerful tools like Dagster, web scraping, Pandas, Plotly, and Jupyter Notebook. It offers a structured way to extract and analyze data from online sources, fostering greater understanding of environmental patterns. By open-sourcing this project, it serves as a reference for individuals aiming to automate their own data analysis and processing pipelines, making complex tasks simpler and more manageable.
