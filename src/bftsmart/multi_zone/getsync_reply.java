package bftsmart.multi_zone;

import bftsmart.clientsmanagement.RequestList;

public class getsync_reply {
    public boolean ok;
    public RequestList list;

    getsync_reply() {
        this.ok = true;
        this.list = new RequestList();
    }

    public void setOk(boolean input) {
        this.ok = input;
    }

    public void setList(RequestList input) {
        this.list = input;
    }

    public boolean getok() {
        return this.ok;
    }

    public RequestList getlist() {
        return this.list;
    }

}
