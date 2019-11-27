# Fuzzy Search
The Soundex algorithm provides a way to search for terms that "sound alike".  This is accomplished by encoding each term with a value.  Words that have similar pronunciation will be encoded with the same value.  This is a simple matching algorithm and does not account for misspellings, typos and other such common user errors.  However, this model can be extended to account for those errors in a similar fashion.

MongoDB provides an easy and convenient method of implementing this type of search.  For each field that needs to be queried via Soundex matching, simply encode the field values and store them in an array in the document.  An index on this array can then support fast and efficient queries in a flexible manner.  The array can contain encoded values from any number of fields at any level of embedding.  This prevents the need to create and manage numerous compound indexes on several fields to support efficient querying.  This is particularly important as the application requirements and document schema changes in the future.

The easiest and most performant way to accomplish this would be to change the application code to encode and update the soundex array values when updating any of the soundex source field values.  Another approach would be to use MongoDB Change Streams to listen for updates in real time that would subsequently update the soundex array.  Finally a bulk update query could also be used to update the soundex array in all of the documents at the same time.  This is the approach taken by this implementation.  However, this is the least performant approach and should only be used on relatively small collections.

## Implementation
The implementation for this demo is a simple Java 1.8 console application.  It provides the following features:
* Build encoded soundex array values from multiple source fields
* Build the index on the soundex array field
* Query the soundex array using $in matching with the specified predicates

## Usage
The application uses the gradle build system.  It will automatically download all necessary dependent libraries.  To build use the appropriate provided gradle wrapper:
Linux/Mac
> ./gradlew build
Windows
> gradlew.bat build

After building, the compiled Java JAR file will be located here:
> build/libs/fuzzy_search.jar

The JAR is "fat" and contains all of the dependent libraries.  It is not necessary to modify the Java class path or provide it on the command line.
Execute the jar file as follows:
> java -jar <path>/fuzzy_search.jar <options>

Here is a full example that:
Builds Soundex encodings for the "name.name" and "name.firstName" fields
Creates the soundex array index (if not already present)
Queries for matches using the predicate "John" on the database fcci and collection cmch.
> java -jar build/libs/fuzzy_search.jar \
>     --uri mongodb://localhost:27017 \
>     -b="name.name" -b="name.firstName" \
>     -s=John -i -d fcci -c cmch

## Options
Multiple options can be combined together in any order.  Most options have default values.  Print out the usage information using the -h option for further information.
* -h
: Print out help and usage information
* -v
* * Print out version information
* --uri <Valid MongoDB connection string>
* -d <database name>
* * Required if database name not specified in connection string
* -c <collection name>
* -b <soundex field name>
* * Format: "field.field2" where field is an array and field2 is the field in the subdocument elements of the array
* * Specify -b multiple times, once for each field that need soundex values encoded
* -i
* * Build index on soundex array
* -s <query predicate>
* * Query soundex array for provided predicate.  The predicate will be encoded by the application.
* * Multiple predicates can be specified by using -s option multiple times.  They will be combined using the $in operator which effectively operates as a logical OR.

## Dependencies
1. MongoDB Synchronous Java Driver version 3.10.1
2. Apache Commons Codec version 1.13
3. picocli version 3.8.2
