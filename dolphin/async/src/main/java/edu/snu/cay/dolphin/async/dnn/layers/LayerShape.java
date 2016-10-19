/*
 * Copyright (C) 2016 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.cay.dolphin.async.dnn.layers;

import javax.inject.Inject;

/**
 * The shape of layer which constructed by channel, height and width.
 */
public final class LayerShape {

  private final int channel;
  private final int height;
  private final int width;

  @Inject
  public LayerShape(final int channel, final int height, final int width) {
    this.channel = channel;
    this.height = height;
    this.width = width;
  }

  public int getChannel() {
    return channel;
  }

  public int getHeight() {
    return height;
  }

  public int getWidth() {
    return width;
  }
}
