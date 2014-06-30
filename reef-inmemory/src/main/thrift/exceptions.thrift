namespace java org.apache.reef.inmemory.fs.exceptions

exception FileNotFoundException{
	1: string message
}

exception FileAlreadyExistsException{
	1: string message
}

exception BlockLoadingException{
    1: i64 timeStarted
}

exception BlockNotFoundException{
    1: string message
}