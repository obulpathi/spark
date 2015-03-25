from pyspark import SparkContext
import csv
import sys
import StringIO


def loadGradesData(fileNameContents):
    """Load all the records in a given file"""
    input = StringIO.StringIO(fileNameContents[1])
    reader = csv.DictReader(input, fieldnames=["name", "class", "grade"])
    return reader


def loadGeoData(fileNameContents):
    """Load all the records in a given file"""
    input = StringIO.StringIO(fileNameContents[1])
    reader = csv.DictReader(input, fieldnames=["name", "place"])
    return reader


def writeRecords(records):
    """Write out CSV lines"""
    output = StringIO.StringIO()
    writer = csv.DictWriter(output, fieldnames=["name", "class", "grade", "place"])
    for record in records:
        writer.writerow(record)
    return [output.getvalue()]


def process(master, gradesFileName, placesFileName, outputFileName):
    sc = SparkContext(master, "LoadCsv")

    # load grades data
    gradesFile = sc.wholeTextFiles(gradesFileName)
    # read the fields
    gradesData = gradesFile.flatMap(loadGrades)
    # filter bad data
    gradesRecords = gradesData.filter(lambda x: x["grade"] != "-")
    # create a map
    gradesMap = gradesRecords.map(lambda x: (x["name"], x))

    # load geo data
    placesFile = sc.wholeTextFiles(placesFileName)
    # read the fields
    placesData = placesFile.flatMap(loadPlacesData)
    # filter bad data
    placesRecords = placesData.filter(lambda x: x["place"] != "-")
    # create a map
    placesMap = placesRecords.map(lambda x: (x["name"], x))

    # join grades and places info
    studentsMap = gradesMap.join(placesMap)

    # group students by class
    groupedStudentsMap = studentsMap.groupedByKey()

    # write output to file
    groupedStudentsMap.saveAstextFile(outputFileName)

    # stop Spark
    sc.stop()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print "Error usage: LoadCsv [sparkmaster] [gradesfile] [placesfile] [outputfile]"
        sys.exit(-1)
    master = sys.argv[1]
    gradesFile = sys.argv[2]
    placesFile = sys.argv[2]
    outputFile = sys.argv[3]
    pricess(master, gradesFile, placesFile, outputFile)
    print "Done!"
