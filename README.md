MarkLogic Bucketeer Native Plugin
-----------------------------------

What it does
------------
It creates 'buckets' in a map object based off of a regular expression. For example,
if you want to bucket based off of the first character of an string you could pass 
"^.". (See Executing for full example)

Requirements
------------

For all Unix systems (Linux, Mac OS X, Solaris), GNU GCC and GNU make utilities
are required.

For Windows systems, Visual Studio 2008 and Cygwin with GNU make utility are
required. For Vista and above (Windows 7, Windows 8, Windows Server 2008+) you
may need to copy this entire directory out of Program Files to another directory
in order to work around Windows security problems writing into Program Files.


Building
--------

For all platforms, run the "make" command from this directory.


Installing
----------
```xquery
xquery version "1.0-ml";
import module namespace plugin = "http://marklogic.com/extension/plugin"
  at "MarkLogic/plugin/plugin.xqy";

plugin:install-from-zip("native",
  xdmp:document-get("<path-to-plugin>/bucketeer-audf.zip")/node())
```

Executing
---------

To execute an aggregate user defined function from the sample plugin, you can
use the cts:aggregate() function, ie:

```xquery
xquery version "1.0-ml";

cts:aggregate(
  "native/bucketeer-audf", 
  "bucketeer", 
  cts:element-reference(xs:QName("name"),"collation=http://marklogic.com/collation/codepoint"),
  "^."
)
```


