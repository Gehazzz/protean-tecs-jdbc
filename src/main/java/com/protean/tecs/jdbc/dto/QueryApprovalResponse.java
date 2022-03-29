package com.protean.tecs.jdbc.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueryApprovalResponse {
    private String query;
    private boolean isApproved;
    private String message;
}
