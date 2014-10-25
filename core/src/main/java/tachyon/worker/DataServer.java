/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.worker;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import com.mellanox.jxio.EventName;
import com.mellanox.jxio.EventQueueHandler;
import com.mellanox.jxio.EventReason;
import com.mellanox.jxio.Msg;
import com.mellanox.jxio.MsgPool;
import com.mellanox.jxio.ServerPortal;
import com.mellanox.jxio.ServerSession;
import com.mellanox.jxio.ServerSession.SessionKey;
import com.mellanox.jxio.WorkerCache.Worker;
import com.mellanox.jxio.exceptions.JxioGeneralException;
import com.mellanox.jxio.exceptions.JxioSessionClosedException;

import tachyon.Constants;
import tachyon.Users;

public class DataServer implements Runnable {

  private static final Logger LOG = Logger.getLogger(Constants.LOGGER_TYPE);
  // The blocks locker manager.
  private final BlocksLocker mBlocksLocker;
  private final EventQueueHandler eqh;
  private final ServerPortal listener;
  private ArrayList<MsgPool> msgPools = new ArrayList<MsgPool>();
  private final int numMsgPoolBuffers = 500;

  class EqhCallbacks implements EventQueueHandler.Callbacks {
    private final DataServer outer = DataServer.this;
    private final int numMsgs;
    private final int inMsgSize;
    private final int outMsgSize;

    public EqhCallbacks(int msgs, int in, int out) {
      numMsgs = msgs;
      inMsgSize = in;
      outMsgSize = out;
    }

    public MsgPool getAdditionalMsgPool(int in, int out) {
      MsgPool mp = new MsgPool(numMsgs, inMsgSize, outMsgSize);
      outer.msgPools.add(mp);
      return mp;
    }
  }
  public DataServer(URI uri, WorkerStorage workerStorage) {
    mBlocksLocker = new BlocksLocker(workerStorage, Users.sDATASERVER_USER_ID);
    MsgPool pool = new MsgPool(numMsgPoolBuffers, 0, 64 * 1024);
    msgPools.add(pool);
    eqh = new EventQueueHandler(new EqhCallbacks(numMsgPoolBuffers, 0, 64 * 1024));
    eqh.bindMsgPool(pool);
    listener = new ServerPortal(eqh, uri, new PortalServerCallbacks(), null);
  }

  public class PortalServerCallbacks implements ServerPortal.Callbacks {
    public void onSessionEvent(EventName session_event, EventReason reason) {
      if (session_event == EventName.PORTAL_CLOSED) {
        eqh.breakEventLoop();
      }
    }

    public void onSessionNew(SessionKey sesKey, String srcIP, Worker workerHint) {
      SessionServerCallbacks callbacks = new SessionServerCallbacks(sesKey.getUri());
      ServerSession session = new ServerSession(sesKey, callbacks);
      callbacks.setSession(session);
      listener.accept(session);
    }
  }

  public class SessionServerCallbacks implements ServerSession.Callbacks {
    private final DataServerMessage responseMessage;
    private ServerSession session;

    public SessionServerCallbacks(String uri) {
      long blockId = Long.parseLong(uri.split("blockId=")[1].split("\\?")[0]);

      int lockId = mBlocksLocker.lock(blockId);
      responseMessage = DataServerMessage.createBlockResponseMessage(true, blockId, 0, -1);
      responseMessage.setLockId(lockId);
    }

    public void setSession(ServerSession ses) {
      session = ses;
    }

    public void onRequest(Msg m) {
      if (session.getIsClosing()) {
        session.discardRequest(m);
      } else {
        responseMessage.copy(m.getOut());
        try {
          session.sendResponse(m);
        } catch (JxioGeneralException e) {
          LOG.error(e.toString());
        } catch (JxioSessionClosedException e) {
          LOG.error(e.toString());
        }
      }

      if (!session.getIsClosing() && responseMessage.finishSending()) {
        session.close();
      }
    }

    public void onSessionEvent(EventName session_event, EventReason reason) {
      if (session_event == EventName.SESSION_CLOSED) {
        responseMessage.close();
        mBlocksLocker.unlock(Math.abs(responseMessage.getBlockId()), responseMessage.getLockId());
      }
    }

    public boolean onMsgError(Msg msg, EventReason reason) {
      LOG.error(this.toString() +  reason);
      return true;
    }
  }

  @Override
  public void run() {
    eqh.runEventLoop(-1, -1);
  }

  public void close() throws IOException {
    listener.close();
  }

  public boolean isClosed() {
    return listener.getIsClosing();
  }
}