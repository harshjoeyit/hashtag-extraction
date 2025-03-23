# Hashtag Extraction Prototype

This project demonstrates a simple system for extracting hashtags from a stream of social media posts, analyzing their frequency, and storing the results in a MongoDB database. It uses RabbitMQ as a message queue to decouple the post generation and hashtag processing components.

## Usage

1.  **Set up RabbitMQ and MongoDB:**
    *   Ensure you have RabbitMQ and MongoDB installed and running.
    *   Create a MongoDB database and collection for storing the hashtag counts.
    ```bash
    > use hashtag
    switched to db hashtag
    > db.createCollection("hashtag")
    ```
2.  **Configure environment variables:**
    1. Create a `.env` file in the project root directory. Run RabbitMQ locally or online and add the same connection url in the .env file
        ```
        QUEUE_URL=<your-queue-url>
        ```
    2. Run Mongodb database locally

3. **Run the project:**

    Naive implementation (No buffering)
    ```
    go run naive/main.go
    ```

    Naive-Batch implementation (Buffer and stop the world to update database)
    ```
    go run naive-batch/main.go naive-batch/buffer.go
    ```

    Naive-Batch-DeepCopy implementation (Copy the buffer to be consumed for database updates)
    ```
    go run naive-batch-deep-copy/main.go naive-batch-deep-copy/buffer.go   
    ```

    Efficient implementation (Swapping the buffers)
    ```
    go run efficient-batch/main.go efficient-batch/buffer.go 
    ```