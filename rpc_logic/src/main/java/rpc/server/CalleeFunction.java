package rpc.server;

import java.io.IOException;

/**
 * Created by slgu1 on 9/13/15.
 */
interface CalleeFunction{
    public String dealRequest(byte [] encodedata) throws IOException;
}
