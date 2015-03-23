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

    gradesData = gradesFile.flatMap(loadGrades)

    goodGradesData = gradesData.filter(
        lambda x: x['grade'] != '-')

    # load geo data
    placesFile = sc.wholeTextFiles(placesFileName)

    placesData = placesFile.flatMap(loadPlacesData)

    goodPlacesData = placesData.filter(
        lambda x: x['place'] != '-')


    fullFilePandaLovers.mapPartitions(
        writeRecords).saveAsTextFile(outputFile + "fullfile")
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
