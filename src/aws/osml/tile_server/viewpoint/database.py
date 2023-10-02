from typing import List
from fastapi import HTTPException

from aws.osml.tile_server.viewpoint.models import InternalViewpointState, ViewpointSummary


class ViewpointStatusTable:

    def __init__(self):
        self.persistent_state = {}

    def update_state(self, new_state: InternalViewpointState):
        self.persistent_state[new_state.description.viewpoint_id] = new_state

    def get_state(self, viewpoint_id: str) -> InternalViewpointState:
        return self.persistent_state.get(viewpoint_id)
    
    def delete_state(self, viewpoint_id: str):
        try:
            del self.persistent_state[viewpoint_id]
        except Exception as err:
            raise HTTPException(status_code=500, detail=f"Tile Server was not able to delete this item! Error: {err}")

    def list_viewpoints(self) -> List[ViewpointSummary]:
        return [ViewpointSummary.from_description(internal_state.description)
                for internal_state in self.persistent_state.values()]
