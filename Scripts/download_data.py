import sys
from utils.functions import download_bajas, download_fleet

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 download_tramits.py [mat|bajas|exact_fleet|all] in order to dowloand all the matriculaciones, bajas, the exact fleet or all of them")
        sys.exit(1)

    argument = sys.argv[1]

    if argument == "bajas":
        print('Let\'s proceed with the download of just the de-registrations')
        download_bajas()

    elif argument == "exact_fleet":
        print('Let\'s proceed with the download the exact vehicle fleet')
        download_fleet()

    elif argument == "all":
        print('Let\'s proceed with the download of all the files')
        download_bajas()
        download_fleet()

    else:
        print('Invalid argument. Use \'bajas\', \'exact_file\' or \'all\'')