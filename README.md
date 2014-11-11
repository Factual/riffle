![](docs/Card-Shuffle.gif)

Riffle is a read-only key/value storage format, strongly influenced by the [cdb](http://cr.yp.to/cdb.html) and [sorted-string table](https://www.igvita.com/2012/02/06/sstable-and-log-structured-storage-leveldb/) formats.  Like cdb, it has a fixed memory cost per key (12 bytes per key), rather than having to keep the entire keyspace in memory.  Like sorted-string tables, it allows for block compression of the values, and allows for files to be merged in linear time.  Like both formats, a typical lookup requires a single disk read.

Riffle files can be built either locally or via Hadoop, allowing for datasets comprising billions of entries to be compiled into a set of sharded Riffle riffles.

### getting started

To use Riffle in your project, add this to your project.clj:

```clj
[factual/riffle "0.1.2"]
```

To use the `riffle` command-line tool, clone the Riffle repository, make sure [Leiningen](https://github.com/technomancy/leiningen) is installed, and then install the tool:

```
cd /tmp
git clone https://github.com/Factual/riffle.git
cd riffle
./scripts/install.sh DIRECTORY
```

where `DIRECTORY` is a directory on your working `$PATH`.  Now you can use the `riffle` tool to build, read, validate and benchmark files.

Let's build a small Riffle file using a TSV key/value file:

```
echo -e "1\t2\n\3\t4\n" | riffle build > /tmp/riffle
```

This is equivalent to the map `{"1" "2", "3" "4"}`.  Now we can do simple things like list the keys in the file, and look up values:

```
> riffle -k /tmp/riffle
3
1

> riffle -g 3 /tmp/riffle
4
```

We can pass in an arbitrary number of Riffle files, in which case the right-most files will take precedence:

```
> echo -e "3\t5" | riffle build > /tmp/riffle2

> riffle -k /tmp/riffle /tmp/riffle2
3
4
1

> riffle -g 3 /tmp/riffle /tmp/riffle2
5

> riffle -g 3 /tmp/riffle2 /tmp/riffle
4
```

We can also arbitrarily combine TSV and Riffle files to create new Riffle files.

```
> echo -e "1\t42" > /tmp/input.tsv

> riffle build /tmp/riffle /tmp/riffle2 /tmp/input.tsv > /tmp/riffle3

> riffle -g 1 /tmp/riffle3
42
```

Riffle stores keys and values as binary data, but for the convenience of the command-line tool all data is treated as plaintext.  To build a file with binary data, you can specify that the input is Base64 encoded with the `-b` flag:

```
> echo -e "`echo -n hello | base64`\t`echo -n goodbye | base64`" > /tmp/binary.tsv

> riffle build -b /tmp/binary.tsv > /tmp/binary-riffle

> riffle -kb /tmp/binary-riffle
aGVsbG8

> riffle -k /tmp/binary-riffle
hello
```

Additional tasks include `validate` and `benchmark`

```
> riffle validate /tmp/binary-riffle
1 block(s), 67.00 average bytes per compressed block
no bad blocks

> riffle benchmark /tmp/binary-riffle
with 1 reader:
throughput: 28456.82 reads/sec
latencies (in ms):
  25.0%  0.03
  50.0%  0.03
  75.0%  0.03
  90.0%  0.04
  95.0%  0.05
  99.0%  0.07
  99.9%  0.19

...
```

### riffle and hadoop

To compile a Riffle index via Hadoop, you can use `riffle hadoop build src1 src2 ... srcN dst`, which takes tab-delimited text input files and builds Riffle indices, and `riffle hadoop merge src1 src2 ... srcN dst`, which takes multiple Riffle indices and merges them together, with precedence given to the right-most index.  These command must be run in a context where the Hadoop environment is already configured.

To build from a source other than tab-delimited files, it's recommended that you customize the mapper for the [RiffleBuildJob](https://github.com/Factual/riffle/blob/master/riffle-hadoop/src/riffle/hadoop/RiffleBuildJob.java#L144-L159), which is trivial to modify.  Once modified, your custom Hadoop job can be installed via `scripts/install.sh`, and invoked via the same `riffle hadoop ...` mechanism.

### riffle as a library

To build a Riffle index at runtime, use `riffle.write/write-riffle`, which takes a sequence of key/value tuples, an output file, and an optional set of parameters.

```clj
> (require '[riffle.write :as w] '[riffle.read :as r])
nil
> (write-riffle [["a" "b"] ["c" "d"]] "/tmp/riffle4")
#<File /tmp/riffle4>
```

This file may be loaded as an index using `riffle.read/riffle` and accessed via `riffle.read/get` and `riffle.read/entries`:

```clj
> (def riff (r/riffle "/tmp/riffle4"))
#'riff
> (r/get riff "a")
#<byte[] [B@6a2361b0>
```

Notice that `get` returns a binary representation of the value.

### license

Copyright © 2014 Factual, Inc

Distributed under the Eclipse Public License v1.0
