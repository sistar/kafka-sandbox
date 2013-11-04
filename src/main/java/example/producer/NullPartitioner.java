package example.producer;

public class NullPartitioner implements kafka.producer.Partitioner {

    public NullPartitioner(kafka.utils.VerifiableProperties properties){

    }

	public int partition(Object key, int a_numPartitions) {
		String keyS = (String) key;
		return 0;
	}

}
