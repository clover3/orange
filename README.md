git clone git@141.223.82.189:beomseok0203/orange.git
cd orange
source script.sh

<if master>
master <slave number>

<else if slave>
slave <master_ip:port> -I <input_dir> -T <temp_dir> -O <output_dir>

<if you want to execute slave briefly..>
I add -t option in slave script. you can modify it and use it.
slave -t [unique_name]
default is 
slave 192.168.10.200:5959 -I /scratch1/Orange/inputdir1 -T /scratch1/Orange/temp" + [unique_name] + " -O /scratch1/Orange/outdir" + [unique_name]

input_dir,temp_dir and output_dir must be expressed in absolute path.
The parent directory of temp_dir and output_dir must exist.
You don't need to make temp_dir and output_dir directory itself.

