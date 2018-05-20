# For the rope method, data structures are used with constant access time so that multiple if-else statements as well as
# a number of unnecessary operations aren't necessary.
rope_mapper = {
    "A": "C",
    "B": "1",
    "C": "2",
    "F": "C",
    "a": "C",
    "b": "1",
    "c": "2",
    "f": "C",

    "0": "5",
    "1": "A",
    "2": "B",
    "3": "5",
    "4": "A",
    "5": "B",
    "6": "5",
    "7": "A",
    "8": "B",
    "9": "5",
}

bucket_mapper = {
    "1": "2",
    "2": "4",
    "3": "6",
    "4": "8",
    "5": "10",

    "6": "4",
    "7": "5",
    "8": "6",
    "9": "7",
}


def shovel(clue, num_times):
    result = clue
    # Initially sort the string to determine the very first addition.
    result = "".join(sorted(result))
    # Add the first set of strings.
    result = result[1:] + ("0A2B3C" if result[0].isdigit() else "1B2C3D")
    # Add the digit string as many times as the total count - 2, due to the special cases of the first and last
    # operations. This string is guaranteed to be the string that's added every time henceforth.
    result = result + ("0A2B3C" * (num_times - 2))
    # Sort one last time, remove all the "first characters" and then add the digit string.
    result = "".join(sorted(result))[num_times - 1:] + "0A2B3C"
    return result


def rope(clue):
    return "".join(rope_mapper.get(x, x) for x in clue)


def torch(clue):
    x = sum(int(i) for i in clue if i.isdigit())
    x = str(x ** 2 if x < 100 else x)
    x = ("F9E8D7" + x[1:]) if len(x) < 10 else (x[6:] + "A1B2C3")
    return x


def bucket(clue):
    return "".join(bucket_mapper.get(x, x) for x in clue)
