/*
 *
 *  Copyright 2016 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package how.hollow.consumer.history;

import com.netflix.hollow.api.consumer.HollowConsumer;
import com.netflix.hollow.api.custom.HollowAPI;
import com.netflix.hollow.core.read.engine.HollowReadStateEngine;
import com.netflix.hollow.tools.history.HollowHistory;


public class ConsumerHistoryListener implements HollowConsumer.RefreshListener {
	
    private HollowHistory history = null;
    
	public HollowHistory getHistory() {
		return history;
	}

    @Override
	public void snapshotUpdateOccurred(HollowAPI api, HollowReadStateEngine stateEngine, long version) throws Exception { 
		if(history == null) {
			history = new HollowHistory(stateEngine, version, 512);
			/// the following allows us to search for changes on IDs using the Lookup Key field
			history.getKeyIndex().indexTypeField("Movie", "id");
			history.getKeyIndex().indexTypeField("Actor", "actorId");
		} else {
			history.doubleSnapshotOccurred(stateEngine, version);
		}
	}
	
	@Override
	public void deltaUpdateOccurred(HollowAPI api, HollowReadStateEngine stateEngine, long version) throws Exception {
		history.deltaOccurred(version);
	}
	
	@Override public void refreshStarted(long currentVersion, long requestedVersion) { }
	@Override public void refreshSuccessful(long beforeVersion, long afterVersion, long requestedVersion) { }
	@Override public void refreshFailed(long beforeVersion, long afterVersion, long requestedVersion, Throwable failureCause) { }
	@Override public void blobLoaded(HollowConsumer.Blob transition) { }
	
}
