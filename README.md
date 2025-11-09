# Flam-backend-assignment-
Building a CLI-based background job queue system:
This is a simple python powered background job queue system with a "Streamlit CLI interface" i have used Streamlit for providing a very basic and minimal UI for executing the commands specified for the job queue system. It supports enqueuing jobs, running workers, retrying failed jobs with exponential backoff along with managing  a DLQ(Dead Letter Queue).

1)**SETUP INSTRUCTIONS:**
   - Python 3.8+
   - Install Dependencies (Streamlit, sqlite3, threading, time, json, uuid, subprocess)
   - note* make sure to install all the dependencies into the same address path as the pyhton in your computer.
   - 
2) **USAGE EXAMPLES:**
   - EXAMPLE 1:
      input : queuectl enqueue {"id":"job1","command":"echo 'Hello World'","max_retries":3}
      outpur : enqueued job1
   - EXAMPLE 2:
      input : queuectl worker start --count 2
      output: started 2 worker(s)
     
3)**Architecture Overview:**
    COMPONENTS:
    - Jobs table (SQLite): stores all the jobs with their states, attemps and retry settings.
    - Workers (Threads): Each worker executes commands and updates job states.
    - Retries and backoffs: Failed jobs retry automatically with exponential backoff {backoff = base_backoff_seconds * (2 ^ (attempt - 1))}.
    - Dead Letter Queue(DLQ): Jobs that fail, all retires are moved here.
    
4) **Assumptions and Trade offs:**
   DOMAIN                                      DECISION MADE                            TRADE OFF
   ----------------------------------------------------------------------------------------------------------------------------------
   Workers                                     implemented as threads                   Limited to one process

   Commands                                    subprocess.run(shell=True) is            It is potentially unsafe but it is flexible.
                                               what it is being executed with.

   Retries                                     Exponential backoff                      Minimal control, only suited for demo scale.

   UI                                          Streamlit                                Very basic could be more engaging.

6) **Testing Instructions:**
   BASIC FLOW:
   - Enqueue a job : queuectl enqueue {"id":"job1","command":"echo Hello","max_retries":3}
   - Start a worker : queuectl worker start --count 1
   - Check status : queuectl status
   TEST DLQ:
    - Limit retries and backoff: queuectl config set max_retries 1 [AND] queuectl config set base_backoff_seconds 1.
    - Enqueue a job with 100% fail rate : queuectl enqueue {"id":"fail1","command":"python -c \"import sys; sys.exit(1)\""}
    - Start a worker: queuectl worker start --count 1
    - Check Status : queuectl dlq list
      
7) **CLEANUP:**
   To reset everything : rm queuectl.db

8) DEMO DRIVE LINK: https://drive.google.com/file/d/19EJMzK3hRVr7TaywY0lCxVmxQiQzm0gN/view?usp=drive_link

