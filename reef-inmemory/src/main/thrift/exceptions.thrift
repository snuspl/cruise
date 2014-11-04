namespace java org.apache.reef.inmemory.common.exceptions

/**
 * File was not found at the specified path
 */
exception FileNotFoundException{
	1: string message
}

/**
 * File cannot be created because it already exists at the specified path
 */
exception FileAlreadyExistsException{
	1: string message
}

/**
 * Generic IO Exception
 */
exception IOException{
    1: string message
}

/**
 * The Cache block could not be retrieved, because it is loading from the Base FS
 */
exception BlockLoadingException{
    1: i64 bytesLoaded,
}

/**
 * The Cache block could not be retrieved, because it was not found
 */
exception BlockNotFoundException{
    1: string message
}

/**
 * The Cache block is not able to write data received from the Client
 */
exception BlockNotWritableException{
    1: string message
}