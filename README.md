##Methods used to parse USPTO patent data files

*Nathan Goldschlag*

*August 17, 2014*

*Version 1.0*


This python project processes the various types of data files from the United States Patent and Trademark office. The PTO has digitized records from 1976 forward. These files, however, come in three different configurations. 1976-2001 are yearly '.dat' files with each date element delineated by a carriage return. 2002-2004 are '.xml' but are slightly different than the 2005-2014 '.xml' files. 

This python script parses these data files and creates a series of yearly .json files with python dictionaries containing a subset of the patent data. These .json files can then be cycled in and out of memory for further processing. This script is focused on the abstract text, and therefore the final dictionaries contain the patent number, patent title, and patent abstract. The methods could easily be modified to capture additional pieces of data.

Execution: execute via terminal. The script will prompt the user to define ('y' or 'n') which processes to run. The primary processes, which are somewhat self-explanatory, are:
1) process 1976-2001
2) process 2002-2004
3) process 2005-2014
4) collapse monthly files

These files can be requested on disk format from the USPTO or downloaded online via the following link.
http://patents.reedtech.com/pgrbbib.php (accessed 8/17/2014)

License: This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation; either version 3 of the License, or (at your option) any later version. Included libraries are subject to their own licenses.
