import com.sun.org.apache.regexp.internal.RE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.locks.Lock;

/**
 * The Lock Manager handles lock and unlock requests from transactions. The
 * Lock Manager will maintain a hash table that is keyed on the resource
 * being locked. The Lock Manager will also keep a FIFO queue of requests
 * for locks that cannot be immediately granted.
 */
public class LockManager {

    public enum LockType {
        S,
        X,
        IS,
        IX
    }

    private HashMap<Resource, ResourceLock> resourceToLock;

    public LockManager() {
        this.resourceToLock = new HashMap<Resource, ResourceLock>();

    }

    /**
     * The acquire method will grant the lock if it is compatible. If the lock
     * is not compatible, then the request will be placed on the requesters
     * queue.
     * @param transaction that is requesting the lock
     * @param resource that the transaction wants
     * @param lockType of requested lock
     */
    public void acquire(Transaction transaction, Resource resource, LockType lockType)
            throws IllegalArgumentException {
        if (!resourceToLock.containsKey(resource)) {
            if (resource.getResourceType() == Resource.ResourceType.PAGE) {
                if((lockType == LockType.IX || lockType == LockType.IS)) {
                    throw new IllegalArgumentException();
                }

                if (lockType == LockType.X) {

                    if(resourceToLock.isEmpty()) {
                        throw new IllegalArgumentException();
                    }
                    Page newResource = (Page)resource;
                    for (Request request: resourceToLock.get((newResource).getTable()).lockOwners) {
                        if (request.transaction.equals(transaction) && request.lockType != LockType.IX) {
                            throw new IllegalArgumentException();
                        }
                    }


                }
                if (lockType == LockType.S) {

                    if (resourceToLock.isEmpty()) {
                        throw new IllegalArgumentException();
                    }
                    Page newResource = (Page)resource;
                    for (Request request: resourceToLock.get((newResource).getTable()).lockOwners) {
                        if (request.transaction.equals(transaction)) {
                            if (!(request.lockType == LockType.IX || request.lockType == LockType.IS)) {
                                throw new IllegalArgumentException();
                            }
                        }
                    }
                }

            }
            if (transaction.getStatus() == Transaction.Status.Waiting) {
                throw new IllegalArgumentException();
            }
            ResourceLock resourceLock = new ResourceLock();
            resourceLock.lockOwners.add(new Request(transaction,lockType));
            resourceToLock.put(resource, resourceLock);

        } else {

            if (transaction.getStatus() == Transaction.Status.Waiting) {
                throw new IllegalArgumentException();
            }


            for (Request request : resourceToLock.get(resource).lockOwners) {
                if ( request.transaction.equals(transaction) && request.lockType.equals(lockType) ) {
                    throw new IllegalArgumentException();
                }
            }

            if (resource.getResourceType() == Resource.ResourceType.TABLE) {
                for (Request request : resourceToLock.get(resource).lockOwners) {
                    if(request.transaction.equals(transaction)) {
                        if (request.lockType.equals(LockType.IX) && lockType == LockType.IS) {
                            throw new IllegalArgumentException();
                        }
                    }

                }
            }
            for (Request request : resourceToLock.get(resource).lockOwners) {
                if (request.transaction.equals(transaction) && request.lockType.equals(LockType.X) && lockType == LockType.S) {
                    throw new IllegalArgumentException();
                }
            }

            if (resource.getResourceType() == Resource.ResourceType.PAGE) {
                if (lockType == LockType.IX || lockType == LockType.IS) {
                    throw new IllegalArgumentException();
                }

            }

            if (resource.getResourceType() == Resource.ResourceType.PAGE) {

                if(lockType == LockType.X){
                    Page newResource = (Page) resource;
                    for (Request r: resourceToLock.get(newResource.getTable()).lockOwners) {
                        if (r.transaction.equals(transaction) && r.lockType != LockType.IX) {
                            throw new IllegalArgumentException();
                        }
                    }
                }
                if(lockType == LockType.S) {
                    Page newResource = (Page) resource;
                    for (Request request: resourceToLock.get(newResource.getTable()).lockOwners) {
                        if (request.transaction.equals(transaction)) {
                            if (!(request.lockType == LockType.IS || request.lockType == LockType.IX)) {
                                throw new IllegalArgumentException();
                            }
                        }
                    }}
            }

            for (Request request : resourceToLock.get(resource).lockOwners) {
                if (request.transaction.equals(transaction)) {
                    if (request.lockType == LockType.S && lockType == LockType.X) {
                        if(resourceToLock.get(resource).lockOwners.size() == 1) {
                            request.lockType = LockType.X;
                        } else {
                            request.lockType = LockType.X;

                            if (!compatible(resource, transaction, lockType)) {
                                request.lockType = LockType.S;
                            }
                        }

                    }
                }
            }

            if (compatible(resource, transaction, lockType)) {
                resourceToLock.get(resource).lockOwners.add(new Request(transaction, lockType));
            } else {
                transaction.setStatus(Transaction.Status.Waiting);
                resourceToLock.get(resource).requestersQueue.add(new Request(transaction,lockType));
            }
        }


    }





        // HW5: To do


    /**
     * Checks whether the a transaction is compatible to get the desired lock on the given resource
     * @param resource the resource we are looking it
     * @param transaction the transaction requesting a lock
     * @param lockType the type of lock the transaction is request
     * @return true if the transaction can get the lock, false if it has to wait
     */
    private boolean compatible(Resource resource, Transaction transaction, LockType lockType) {
        if (resourceToLock.get(resource).lockOwners.isEmpty()) {
            return true;
        }
        for (Request request: resourceToLock.get(resource).lockOwners) {
            if (request.lockType.equals(LockType.IS) && lockType == LockType.IS) {
                return true;
            } else if (request.lockType.equals(LockType.IS) && lockType == LockType.S) {
                return true;
            } else if (request.lockType.equals(LockType.IX) && lockType == LockType.IX) {
                return true;
            } else if (request.lockType.equals(LockType.S) && lockType == LockType.S) {
                return true;
            } else if (request.lockType.equals(LockType.IS) && lockType == LockType.IX) {
                return true;
            } else if (request.lockType.equals(LockType.S) && lockType == LockType.IS) {
                return true;
            } else if (request.lockType.equals(LockType.IX) && lockType == LockType.IS) {
                return true;
            } else {
                return false;
            }
        }
        return false;
    }




    /**
     * Will release the lock and grant all mutually compatible transactions at
     * the head of the FIFO queue. See spec for more details.
     * @param transaction releasing lock
     * @param resource of Resource being released
     */
    public void release(Transaction transaction, Resource resource) throws IllegalArgumentException{
        if(!resourceToLock.containsKey(resource)) {
            throw new IllegalArgumentException();
        }

        if(transaction.getStatus() == Transaction.Status.Waiting) {
            throw new IllegalArgumentException();
        }



        if(resource.getResourceType() == Resource.ResourceType.TABLE) {
            Table newResource = (Table) resource;
            for (Page page: newResource.getPages() ){
                for (Request request: resourceToLock.get(page).lockOwners) {
                    if (request.transaction == transaction) {
                        throw new IllegalArgumentException();
                    }
                }

            }
        }
        for(Request request: resourceToLock.get(resource).lockOwners) {
            if(!request.lockType.equals(LockType.IS)  &&!request.lockType.equals(LockType.X)
            &&!request.lockType.equals(LockType.S)  && !request.lockType.equals(LockType.IX)) {
                throw new IllegalArgumentException();
            }
        }



        for (Request request : resourceToLock.get(resource).lockOwners) {
                if (request.transaction.equals(transaction)) {
                    resourceToLock.get(resource).lockOwners.remove(request);

                    break;
                }

            }

            if (resourceToLock.get(resource).lockOwners.isEmpty()) {
                for (Request request : resourceToLock.get(resource).requestersQueue) {
                    resourceToLock.get(resource).lockOwners.add(request);
                    request.transaction.wake();
                    resourceToLock.get(resource).requestersQueue.remove(request);
                    break;
                }
            }




            if(resourceToLock.get(resource).lockOwners.size() == 1
                    && resourceToLock.get(resource).lockOwners.get(0).lockType == LockType.S) {
                for(Request r: resourceToLock.get(resource).requestersQueue) {
                    if(r.transaction.equals(resourceToLock.get(resource).lockOwners.get(0).transaction)
                            && r.lockType == LockType.X) {
                        resourceToLock.get(resource).lockOwners.get(0).lockType = LockType.X;
                        resourceToLock.get(resource).requestersQueue.remove(r);
                        resourceToLock.get(resource).lockOwners.get(0).transaction.wake();

                    }
                }
            }
            Iterator<Request> iter = resourceToLock.get(resource).requestersQueue.iterator();
            LinkedList<Request> rmqueue = new LinkedList<>();
            while (iter.hasNext()) {

                Request temp = iter.next();
                if (compatible(resource, temp.transaction, temp.lockType)) {
                    rmqueue.add(temp);
                    temp.transaction.wake();
                    resourceToLock.get(resource).lockOwners.add(temp);
                } else {
                    break;
                }
            }
            for (Request r : rmqueue) {
                resourceToLock.get(resource).requestersQueue.remove(r);
                if (!compatible(resource, r.transaction, r.lockType)) {
                    break;
                }
            }


        }





        // HW5: To do


    /**
     * This method will grant mutually compatible lock requests for the resource
     * from the FIFO queue.
     * @param resource of locked Resource
     */
     private void promote(Resource resource) {

         return;
     }

    /**
     * Will return true if the specified transaction holds a lock of type
     * lockType on the resource.
     * @param transaction potentially holding lock
     * @param resource on which we are checking if the transaction has a lock
     * @param lockType of lock
     * @return true if the transaction holds lock
     */
    public boolean holds(Transaction transaction, Resource resource, LockType lockType) {
        for (Request request : resourceToLock.get(resource).lockOwners) {
            if( request.transaction.equals(transaction)  && request.lockType.equals( lockType) ) {
                return true;
            }

        }
        return false;

        // HW5: To do
    }

    /**
     * Contains all information about the lock for a specific resource. This
     * information includes lock owner(s), and lock requester(s).
     */
    private class ResourceLock {
        private ArrayList<Request> lockOwners;
        private LinkedList<Request> requestersQueue;

        public ResourceLock() {
            this.lockOwners = new ArrayList<Request>();
            this.requestersQueue = new LinkedList<Request>();
        }

    }

    /**
     * Used to create request objects containing the transaction and lock type.
     * These objects will be added to the requester queue for a specific resource
     * lock.
     */
    private class Request {
        private Transaction transaction;
        private LockType lockType;

        public Request(Transaction transaction, LockType lockType) {
            this.transaction = transaction;
            this.lockType = lockType;
        }

        @Override
        public String toString() {
            return String.format(
                    "Request(transaction=%s, lockType=%s)",
                    transaction, lockType);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            } else if (o instanceof Request) {
                Request otherRequest  = (Request) o;
                return otherRequest.transaction.equals(this.transaction) && otherRequest.lockType.equals(this.lockType);
            } else {
                return false;
            }
        }
    }
}
