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



