This code is to copy content from one bucket to another, in 
our case for backup.

The main idea is that our source bucket has a lambda attached
to it that is called on writes. The lambda merely copies the
written object to the backup bucket. In our case, the backup
bucket also has a policy which sends things to Glacier storage.

The buckets are versioned and nothing is done on deletes, so 
we get a backup of all versions of the objects.

The main complication is that some (only a few, but some) 
objects are too big to copy within the 15 minute lifespan 
of a lambda. So what do we do with them? Our idea is to
dispatch on the size of the object. If it is too large to
be comfortable to copy in the given time then we send a
message to a queue instead of copying. Another service
running on one of our EC2 instances (most likely just run
by cron) polls the queue. If it finds a message there
then it does the copy - without the restriction. 

Another possible complication is if there is an error in 
copying, either in the lambda or in the alternate service.
We may deal with this through an augmentation of the 
messaging process - I haven't really come to a conclusion
here yet.

So we need to have a reasonably robust method of dealing with
these sorts of issues.

Ideally, we'll also develop something that has the ability to
compare what is in the two buckets periodically to make sure
that nothing has slipped through the cracks. This will probably
involve getting a bucket version inventory of each, cross-checking,
and producing a report.

Just for thought for a simple system. The lambda could always
send a message, including a unique id, when it gets an event. 
If the object is small enough, then it will attempt to copy.
If not, then it will do nothing. If a copy completes, then
it sends another message (with the same unique id) indicating
success.

The alternate service pulls messages from the queue. It creates
a record for start messages and destroys it for end messages.
When the queue is empty, then it searches for start messages
old enough to imply failure. It then copies these, and on 
success, removes the record. On failure here, we can easily log.

Note that it is possible (I think) for the messages to arrive
out of order in SQS. So we need to account for this as well. 