package eu.activelogic.key.correlation;

public class TemporalCorrelatingKey {

	private final long base;
	private final long adjustment;

	public TemporalCorrelatingKey(long base, long adjustment) {
		super();
		this.base = base;
		this.adjustment = adjustment;
	}

	public long generateBaseKey(long time) {
		return generateBaseKey(time, base, adjustment);
	}

	public long generateTargetKey(long time) {
		return generateTargetKey(time, base, adjustment);
	}

	public static long generateBaseKey(long time, long base, long adjustment) {
		return time - time % base;
	}

	public static long generateTargetKey(long time, long base, long adjustment) {
		return (time + adjustment) - (time + adjustment) % base;
	}

}
