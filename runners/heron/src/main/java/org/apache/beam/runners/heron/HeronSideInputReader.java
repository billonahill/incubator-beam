package org.apache.beam.runners.heron;

import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.SideInputReader;
import org.apache.beam.sdk.util.WindowingStrategy;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class HeronSideInputReader implements SideInputReader {
  private final List<PCollectionView<?>> sideInputs;

  public HeronSideInputReader(List<PCollectionView<?>> sideInputs) {
    this.sideInputs = sideInputs;
  }

  @Nullable
  @Override
  public <T> T get(PCollectionView<T> view, BoundedWindow window) {
    throw new UnsupportedOperationException("HeronSideInputReader.get not yet implemented");
  }

  @Override
  public <T> boolean contains(PCollectionView<T> view) {
    return sideInputs.contains(view.getTagInternal());
  }

  @Override
  public boolean isEmpty() {
    return sideInputs != null && sideInputs.isEmpty();
  }
}