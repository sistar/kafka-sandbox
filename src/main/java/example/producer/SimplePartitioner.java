package example.producer;

public class SimplePartitioner implements kafka.producer.Partitioner {

    public SimplePartitioner(kafka.utils.VerifiableProperties properties){

    }

	public int partition(Object key, int a_numPartitions) {
		String keyS = (String) key;
		int partition = 0;
		int offset = keyS.lastIndexOf('.');
		if (offset > 0) {
			partition = Integer.parseInt(keyS.substring(offset + 1)) % a_numPartitions;
		}
		return partition;
	}

}
