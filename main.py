import threading
import requests

# Number of threads to use
<<<<<<< HEAD
NUM_THREADS = 1000

=======
NUM_THREADS = 10000
count = 0
>>>>>>> 1d44c7c (update)
# Function to send a GET request
def send_request():
  try:
      response = requests.get('http://localhost:8080', stream=True)
<<<<<<< HEAD
      print(f'Response code: {response.status_code}')
=======
      #print(f'Content: {response.content}')
>>>>>>> 1d44c7c (update)
  except requests.exceptions.RequestException as e:
      print(f'Error: {e}')

# Create and start the threads
threads = []
for i in range(NUM_THREADS):
    t = threading.Thread(target=send_request)
    threads.append(t)
    t.start()

# Wait for all threads to finish
for t in threads:
    t.join()

print('All threads finished')

