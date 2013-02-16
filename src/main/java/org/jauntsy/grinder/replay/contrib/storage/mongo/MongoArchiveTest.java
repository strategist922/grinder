package org.jauntsy.grinder.replay.contrib.storage.mongo;

import backtype.storm.utils.Utils;
import com.exacttarget.kafka.testing.LocalKafkaBroker;
import org.jauntsy.grinder.replay.api.ArchiveClient;
import org.jauntsy.grinder.replay.api.ArchiveConfig;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

import java.io.IOException;

/**
 * User: ebishop
 * Date: 12/20/12
 * Time: 12:02 PM
 */
public class MongoArchiveTest {
    public static void main(String[] args) throws IOException {
        LocalKafkaBroker broker = new LocalKafkaBroker();

        ArchiveConfig conf = new MongoArchiveConfig.Builder().host("localhost");
        ArchiveClient archive = conf.buildArchive(null);

        ArchiveClient.Partition seq = archive.getPartition("test", "localhost", 9090, 0);
        for (int i = 0; i < 100; i++) {
            String msg = "Message " + i;
            seq.send(i, new MessageAndOffset(new Message(msg.getBytes()), i + 1));
        }
        Utils.sleep(1000);

        long offset = 0;
//        while (true) {
            int found = 0;
            for (MessageAndOffset mno : seq.fetch(offset, 10000)) {
                System.out.println("Found " + new String(Utils.toByteArray(mno.message().payload())));
                found++;
                offset = mno.offset();
            }
//            if (found == 0)
//                break;
//        }

        archive.close();
        broker.shutdown();
    }
}
