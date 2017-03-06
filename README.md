## Fries Output Extractor

This is a public code repository of the Computational Language Understanding (CLU) Lab led by [Mihai Surdeanu](http://surdeanu.info/mihai/) at [University of Arizona](http://www.arizona.edu).

Author: [Tom Hicks](https://github.com/hickst)

Purpose: Converts triples of FRIES-format JSON files, containing Reach results, into a simpler
format for use by various other programs.

## Installation

This software requires Java 1.8, Gradle 2.7+, and Groovy 2.4.X+.

To build the standalone JAR file in the build/libs subdirectory:

```
   > gradle clean shadowJar
```

To run the JAR file:

```
   > java -jar frext-1.0.0.jar -v /input/dir/path
OR
   > java -jar frext-1.0.0.jar -v -o output/dir/path /input/dir/path
```

Run Options:

```
Usage: frext [-h] [-m] [-v] [-o output-directory] input-directory

 -h, --help           Show usage information
 -o, --outdir         Directory for output files (default: current directory)
 -m, --map            Map input filenames to PMC IDs (default: no mapping needed)
 -v, --verbose        Run in verbose mode (default: non-verbose)
```

## License

Licensed under Apache License Version 2.0.

(c) The University of Arizona, 2017
