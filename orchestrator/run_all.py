import os
import time

print("--- STARTING FINAL DIAGNOSTIC ---")
print("--- Printing all available environment variables ---")

# Print all environment variables to the log
for key, value in os.environ.items():
    # We will hide the secret values for security, but print the keys
    if "KEY" in key.upper() or "SECRET" in key.upper():
        print(f"{key}=**********")
    else:
        print(f"{key}={value}")

print("--- DIAGNOSTIC COMPLETE ---")
print("--- The application will now sleep for 10 minutes before exiting to allow log inspection. ---")

# Sleep for a long time so the container doesn't exit immediately
time.sleep(600)
