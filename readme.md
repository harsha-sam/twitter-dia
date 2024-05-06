# Real-Time Sentiment Analysis and Recommendation System

## Project Overview

This project implements a real-time sentiment analysis and recommendation system for Twitter-like data. It leverages a microservices architecture incorporating technologies such as Neo4j, MongoDB, Kafka, and Spark. The primary focus is on building a scalable data pipeline capable of handling real-time data streams, performing sentiment analysis, and dynamically updating recommendation outputs based on user interactions and sentiment scores.

## Technologies Used

- **Neo4j**: Graph database for managing complex relationships and providing real-time recommendations.
- **MongoDB**: Document-based database used for storing user profiles and tweet data.
- **Apache Kafka**: Message broker for handling real-time data streaming.
- **Apache Spark**: Processing framework for real-time data analysis and sentiment evaluation.
- **Flask**: Python web framework for handling server-side operations and user interactions.

## Features

- Real-time sentiment analysis of tweets.
- Dynamic recommendation engine for suggesting similar users and hashtags.
- User authentication and profile management using MongoDB.
- Interactive web interface for posting tweets and viewing recommendations.

## Installation

Clone the repository:
`git clone https://github.com/harsha-sam/twitter-dia.git`

### Setting Up the Environment

Ensure you have Docker and docker-compose installed on your machine. Navigate to the project directory and run:


This command will set up all the necessary services and start the application.

`docker-compose up -d`

## Usage

Once all the services are running, navigate to `http://localhost:4000` in your web browser to access the web interface.

- **Login**: Login to access the main features.
- **Post a Tweet**: Enter your thoughts and see real-time sentiment analysis results.
- **View Recommendations**: Check out users and hashtags that match your sentiment preferences.
