# How to Run

## To Checkout

Execute following comamnds in linux shell

`$ git clone git@141.223.82.189:beomseok0203/orange.git`

Now the source code is being checked out. You can run it with pre-compiled jar files.

First, move to orange directory.

`$ cd orange`

Then load the command environment 

`$ source script.sh``

## Running master

To run master, execute the following command 

`$ master [number of slaves]`

For example 

`$ master 5`

## Running slave
To run slave, execute the following command


`$ slave [MasterIP:port] -I [InputDirectory] -T [TempDirectory] -O [OutputDirectory]`

For example 

`$ slave 192.168.10.200:5959 -I /scratch1/Orange/inputdir1 -T /scratch1/Orange/temp1 -O /scratch1/Orange/outdir1`

Note that
 * Master's listening port is fixed : 5959
 * Input directory, Temp directory and Output directory must be absolute path.
 * The parent directory of TempDirectory and OutputDirectory must exist.
 * You don't need to make TempDirectory and OutputDirectory directory itself.

### If you want to execute slave in short command

We add -t option in slave script. you can modify it and use it.

`$ slave -t [unique_name]`

In default, it executes
 
`$ slave 192.168.10.200:5959 -I /scratch1/Orange/inputdir1 -T /scratch1/Orange/temp" + [unique_name] + " -O /scratch1/Orange/outdir" + [unique_name]`

for example if you type 'slave -t 1' it will execute

`$ slave 192.168.10.200:5959 -I /scratch1/Orange/inputdir1 -T /scratch1/Orange/temp1 -O /scratch1/Orange/outdir1`


### Config File

There is a config file in which you can specify performance paramters.

Config file contains two entry. 
* Sort block size
* Number of merge thread
 
#### Sort block size

The first entry specifies the size of the block to be sorted in first sorting phase.

Size of the block is given in number of records. It is recomended to set block size in multiples of single file(327680).

Default value is set to 983040, which is three times the size of single file.

Having larger block size will results in short processing time in the merge process. However, it will require more memory space that you need to allocates more heap space to the program.

#### Number of merge thread 

Merge is processed in parallel. However, the number of threads need to be specified prior to the execution. 
You can specify the degree of parallelism in the merge process. Default value is set to four, which means that 4 concurrent threads will process the merging.
It is recommended to set this value equal to the number of cores in the machine. 
