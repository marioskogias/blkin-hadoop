package hadoopCode;

import java.util.Calendar;

import mythrift.Annotation;
import mythrift.Span;

public class Chooser implements AnnotationChooser{
	
	static final long duration = 900000; // 15 min in milisecs
	static final String span1 = "parent process";
	static final String span2 = "parent process";
	static final String annot1 = "child start";
	static final String annot2 = "child end";

	static final long threshold = Calendar.getInstance().getTimeInMillis()
			- duration;

	@Override
	public boolean shouldKeepSpan(Span s) {
		return (s.name.equals(span1) || s.name.equals(span2));
	}

	@Override
	public boolean shouldKeepAnnotation(Annotation a) {
		/*
		 * keep last quarter annotations timestamp in usec so divide by 1000
		 */
		return ((a.value.equals(annot1) || a.value.equals(annot2)) && ((a.timestamp / 1000) > threshold));

	}

	@Override
	public String getId(Span s, Annotation a) {
		return Long.toString(s.trace_id) + ":" + Long.toString(s.id);
	}

}
