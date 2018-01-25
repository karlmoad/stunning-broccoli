from culc import saywhat

if __name__ == "__main__":

    sw = saywhat()

    print("Attempting to communicate...")
    print("Saying hello")
    print("Response from it: {}".format(sw.communicate("hello")))
