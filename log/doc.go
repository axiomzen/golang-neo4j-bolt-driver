/*Package log implements the logging for the bolt driver

There are 3 logging levels - trace, info and error.  Setting trace would also set info and error logs.
You can use the SetLevel("trace") to set trace logging, for example.
The loggers & level are not thread safe so set the loggers and the level once in your app.
*/
package log
