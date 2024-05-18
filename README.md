# (Non-parametric) Bootstrap Robust Strategies

## Installing conda env

1) Create conda env with python dependencies

    `bash setup_dependencies.sh`

[comment]: <> (## Procedure)

[comment]: <> (1&#41; Install all packages)

[comment]: <> (`bash install.sh`)

2) Generate ETFs Dataset

    `cd src`

    `python build_etfs_data.py`

3) Generate ETFs Dataset

    `cd gym`

    `ulimit -n 64000`
    
    `python training_etfstsm.py`

The argument "ulimit -n 64000" is used to increase the number of times one can open a single file. A reference of the error is below:

> https://stackoverflow.com/questions/34588/how-do-i-change-the-number-of-open-files-limit-in-linux/8285278#8285278
